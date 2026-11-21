using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Unhandled;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.Unhandled;

public sealed class RabbitMqUnhandledMessageStrategyTests
{
    [Fact]
    public async Task HandleAsync_WhenPublishSucceeds_ReturnsAckAndPreservesMessage()
    {
        var context = CreateContext();
        var publisher = new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        BasicProperties? capturedProperties = null;
        ReadOnlyMemory<byte> capturedBody = default;
        publisher
            .Setup(x => x.PublishAsync(
                "asyncomnia/unhandled",
                "unhandled/events/clients/subscription/billing-service",
                true,
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                CancellationToken.None))
            .Callback<string, string, bool, BasicProperties,
                ReadOnlyMemory<byte>, CancellationToken>(
                (_, _, _, properties, body, _) =>
                {
                    capturedProperties = properties;
                    capturedBody = body;
                })
            .Returns(Task.CompletedTask);
        var strategy = CreateStrategy(publisher);

        var response = await strategy.HandleAsync(
            context,
            UnhandledMessageReason.UnknownContract);

        Assert.Equal(AckResponse.Ack, response);
        Assert.NotNull(capturedProperties);
        Assert.Equal(context.Properties.Type, capturedProperties!.Type);
        Assert.Equal(context.Properties.MessageId, capturedProperties.MessageId);
        Assert.Null(capturedProperties.Expiration);
        Assert.NotNull(capturedProperties.Headers);
        Assert.Equal(
            "existing-value",
            capturedProperties.Headers!["existing-header"]);
        Assert.Equal(
            UnhandledMessageReason.UnknownContract,
            capturedProperties.Headers["x-asyncomnia-unhandled-reason"]);
        Assert.Equal(
            context.QueueName,
            capturedProperties.Headers["x-asyncomnia-source-queue"]);
        Assert.Equal(
            context.ExchangeName,
            capturedProperties.Headers["x-asyncomnia-source-exchange"]);
        Assert.Equal(
            context.RoutingKey,
            capturedProperties.Headers["x-asyncomnia-source-routing-key"]);
        Assert.Equal(context.Body.ToArray(), capturedBody.ToArray());
        publisher.VerifyAll();
    }

    [Fact]
    public async Task HandleAsync_WhenPublishFails_ReturnsNackWithRequeue()
    {
        var context = CreateContext();
        var publisher = new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        publisher
            .Setup(x => x.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                true,
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                CancellationToken.None))
            .ThrowsAsync(new InvalidOperationException("broker unavailable"));
        var strategy = CreateStrategy(publisher);

        var response = await strategy.HandleAsync(
            context,
            UnhandledMessageReason.UnknownContract);

        Assert.Equal(AckResponse.NackWithRequeue, response);
        publisher.VerifyAll();
    }

    [Fact]
    public async Task HandleAsync_WhenPublishIsCancelled_ReturnsNackWithRequeue()
    {
        var context = CreateContext();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();
        var publisher = new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        publisher
            .Setup(x => x.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                true,
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                cancellationTokenSource.Token))
            .ThrowsAsync(
                new OperationCanceledException(cancellationTokenSource.Token));
        var strategy = CreateStrategy(publisher);

        var response = await strategy.HandleAsync(
            context,
            UnhandledMessageReason.UnknownContract,
            cancellationTokenSource.Token);

        Assert.Equal(AckResponse.NackWithRequeue, response);
        publisher.VerifyAll();
    }

    [Fact]
    public async Task HandleAsync_WhenContextIsNull_Throws()
    {
        var strategy = CreateStrategy(
            new Mock<IRabbitMqPublisher>(MockBehavior.Strict));

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await strategy.HandleAsync(
                null!,
                UnhandledMessageReason.UnknownContract));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task HandleAsync_WhenReasonIsMissing_Throws(string? reason)
    {
        var strategy = CreateStrategy(
            new Mock<IRabbitMqPublisher>(MockBehavior.Strict));

        await Assert.ThrowsAnyAsync<ArgumentException>(async () =>
            await strategy.HandleAsync(CreateContext(), reason!));
    }

    private static RabbitMqUnhandledMessageStrategy CreateStrategy(
        Mock<IRabbitMqPublisher> publisher)
    {
        return new RabbitMqUnhandledMessageStrategy(
            NullLogger<RabbitMqUnhandledMessageStrategy>.Instance,
            new RabbitMqUnhandledRouteResolver(),
            publisher.Object);
    }

    private static RabbitMqConsumeContext CreateContext()
    {
        return new RabbitMqConsumeContext(
            "events/clients/subscription/billing-service",
            "events/clients",
            "client-created-event",
            new BasicProperties
            {
                Type = "Contracts.ClientDeleted, Contracts",
                MessageId = Guid.NewGuid().ToString("D"),
                Expiration = "30000",
                Headers = new Dictionary<string, object?>
                {
                    ["existing-header"] = "existing-value"
                }
            },
            new byte[] { 1, 2, 3 });
    }
}
