using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using EasyNetQ;
using EasyNetQ.Consumer;
using EasyNetQ.Topology;
using Moq;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.DeadLettering;

public sealed class RabbitMqDeadLetterPublisherTests
{
    [Fact]
    public async Task PublishAsync_DeclaresDurableTopologyAndPublishesOriginalMessageWithoutExpiration()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy =
            new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var publisher = new RabbitMqDeadLetterPublisher(
            advancedBus.Object,
            exchangeDeclareStrategy.Object,
            new RabbitMqDeadLetterRouteResolver());
        var exchange = CreateExchange("asyncomnia/deadletter");
        var queue = CreateQueue(
            "deadletter/events/clients/subscription/billing-service");
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
        Action<IQueueDeclareConfiguration>? configureQueue = null;
        exchangeDeclareStrategy
            .Setup(x => x.DeclareExchangeAsync(
                exchange.Name,
                ExchangeType.Topic,
                CancellationToken.None))
            .ReturnsAsync(exchange);
        advancedBus
            .Setup(x => x.QueueDeclareAsync(
                queue.Name,
                It.IsAny<Action<IQueueDeclareConfiguration>>(),
                CancellationToken.None))
            .Callback<string, Action<IQueueDeclareConfiguration>, CancellationToken>(
                (_, configure, _) => configureQueue = configure)
            .ReturnsAsync(queue);
        advancedBus
            .Setup(x => x.QueueBindAsync(
                queue.Name,
                exchange.Name,
                queue.Name,
                null,
                CancellationToken.None))
            .Returns(Task.CompletedTask);
        advancedBus
            .Setup(x => x.PublishAsync(
                exchange.Name,
                queue.Name,
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

        Assert.NotNull(configureQueue);
        Assert.Equal(expiration, context.Properties.Expiration);
        var queueConfiguration =
            new Mock<IQueueDeclareConfiguration>(MockBehavior.Strict);
        queueConfiguration
            .Setup(x => x.AsDurable(true))
            .Returns(queueConfiguration.Object);
        queueConfiguration
            .Setup(x => x.AsAutoDelete(false))
            .Returns(queueConfiguration.Object);
        queueConfiguration
            .Setup(x => x.AsExclusive(false))
            .Returns(queueConfiguration.Object);

        configureQueue(queueConfiguration.Object);

        queueConfiguration.VerifyAll();
        exchangeDeclareStrategy.VerifyAll();
        advancedBus.VerifyAll();
    }

    [Fact]
    public async Task PublishAsync_WhenCancellationIsAlreadyRequested_DoesNotCallEasyNetQ()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy =
            new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var publisher = new RabbitMqDeadLetterPublisher(
            advancedBus.Object,
            exchangeDeclareStrategy.Object,
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
        exchangeDeclareStrategy.VerifyNoOtherCalls();
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

    private static Exchange CreateExchange(string name)
    {
        return new Exchange(
            name,
            ExchangeType.Topic,
            true,
            false,
            new Dictionary<string, object>());
    }

    private static Queue CreateQueue(string name)
    {
        return new Queue(
            name,
            true,
            false,
            false,
            new Dictionary<string, object>());
    }
}
