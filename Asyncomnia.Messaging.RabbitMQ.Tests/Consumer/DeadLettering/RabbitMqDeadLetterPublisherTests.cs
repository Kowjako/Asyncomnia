using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using EasyNetQ;
using EasyNetQ.Consumer;
using Moq;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.DeadLettering;

public sealed class RabbitMqDeadLetterPublisherTests
{
    [Fact]
    public async Task PublishAsync_PublishesOriginalMessageWithoutExpiration()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var publisher = new RabbitMqDeadLetterPublisher(
            advancedBus.Object,
            new RabbitMqDeadLetterRouteResolver());
        var exchangeName = "asyncomnia/deadletter";
        var queueName =
            "deadletter/events/clients/subscription/billing-service";
        var messageId = Guid.NewGuid().ToString("D");
        var expiration = TimeSpan.FromMinutes(5);
        ReadOnlyMemory<byte> body = new byte[] { 1, 2, 3 };
        var context = CreateContext(
            body,
            new MessageProperties
            {
                MessageId = messageId,
                Expiration = expiration
            });
        advancedBus
            .Setup(x => x.PublishAsync(
                exchangeName,
                queueName,
                true,
                true,
                It.Is<MessageProperties>(properties =>
                    properties.MessageId == messageId &&
                    properties.Expiration == null),
                It.Is<ReadOnlyMemory<byte>>(publishedBody =>
                    publishedBody.Equals(body)),
                CancellationToken.None))
            .Returns(Task.CompletedTask);

        await publisher.PublishAsync(context);

        Assert.Equal(expiration, context.Properties.Expiration);
        advancedBus.VerifyAll();
    }

    [Fact]
    public async Task PublishAsync_WhenCancellationIsAlreadyRequested_DoesNotCallEasyNetQ()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var publisher = new RabbitMqDeadLetterPublisher(
            advancedBus.Object,
            new RabbitMqDeadLetterRouteResolver());
        var context = CreateContext(
            new byte[] { 1 },
            new MessageProperties());
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => publisher.PublishAsync(
                context,
                cancellationTokenSource.Token));

        advancedBus.VerifyNoOtherCalls();
    }

    private static ConsumeContext CreateContext(ReadOnlyMemory<byte> body, MessageProperties properties)
    {
        var receivedInfo = new MessageReceivedInfo(
            "consumer",
            1,
            false,
            "events/clients",
            "client-created-event",
            "events/clients/subscription/billing-service");
        var services = Mock.Of<IServiceProvider>();

        return new ConsumeContext(receivedInfo, properties, body, services, CancellationToken.None);
    }
}
