using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.DeadLettering;

public sealed class RabbitMqDeadLetterPublisherTests
{
    [Fact]
    public async Task PublishAsync_PublishesOriginalMessageWithoutExpiration()
    {
        var brokerPublisher = new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var publisher = new RabbitMqDeadLetterPublisher(
            brokerPublisher.Object,
            new RabbitMqDeadLetterRouteResolver());
        var messageId = Guid.NewGuid().ToString("D");
        ReadOnlyMemory<byte> body = new byte[] { 1, 2, 3 };
        var properties = new BasicProperties
        {
            MessageId = messageId,
            Expiration = "300000"
        };
        var context = CreateContext(body, properties);
        brokerPublisher
            .Setup(x => x.PublishAsync(
                "asyncomnia/deadletter",
                "deadletter/events/clients/subscription/billing-service",
                true,
                It.Is<BasicProperties>(publishedProperties =>
                    publishedProperties.MessageId == messageId &&
                    publishedProperties.Expiration == null),
                It.Is<ReadOnlyMemory<byte>>(publishedBody => publishedBody.Equals(body)),
                CancellationToken.None))
            .Returns(Task.CompletedTask);

        await publisher.PublishAsync(context);

        Assert.Equal("300000", context.Properties.Expiration);
        brokerPublisher.VerifyAll();
    }

    [Fact]
    public async Task PublishAsync_WhenCancellationIsAlreadyRequested_DoesNotPublish()
    {
        var brokerPublisher = new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var publisher = new RabbitMqDeadLetterPublisher(
            brokerPublisher.Object,
            new RabbitMqDeadLetterRouteResolver());
        var context = CreateContext(new byte[] { 1 }, new BasicProperties());
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => publisher.PublishAsync(context, cancellationTokenSource.Token));

        brokerPublisher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task PublishAsync_WhenContextIsNull_Throws()
    {
        var brokerPublisher = new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var publisher = new RabbitMqDeadLetterPublisher(
            brokerPublisher.Object,
            new RabbitMqDeadLetterRouteResolver());

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => publisher.PublishAsync(null!));

        brokerPublisher.VerifyNoOtherCalls();
    }

    private static RabbitMqConsumeContext CreateContext(
        ReadOnlyMemory<byte> body,
        IReadOnlyBasicProperties properties)
    {
        return new RabbitMqConsumeContext(
            "events/clients/subscription/billing-service",
            "events/clients",
            "client-created-event",
            properties,
            body);
    }
}
