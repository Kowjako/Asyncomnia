using Asyncomnia.Messaging.RabbitMQ.Routing;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using Asyncomnia.Messaging.RabbitMQ.Transport;
using EasyNetQ;
using EasyNetQ.Topology;
using Moq;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Transport;

public sealed class EasyNetQOutboundTransportTests
{
    [Fact]
    public async Task DispatchAsync_ForEvent_PublishesToResolvedRouteWithMetadata()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy = new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var deliveryModeStrategy = new Mock<IMessageDeliveryModeStrategy>(MockBehavior.Strict);
        var transport = CreateTransport(advancedBus, exchangeDeclareStrategy, deliveryModeStrategy);
        var @event = new ClientCreatedEvent("Acme");
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(2026, 11, 6, 12, 30, 45, TimeSpan.Zero);
        var metadata = new MessageMetadata(messageId, createdAt)
        {
            CorrelationId = correlationId
        };

        var exchange = CreateExchange("events/clients");
        EasyNetQ.IMessage? publishedMessage = null;
        using var cancellationTokenSource = new CancellationTokenSource();
        exchangeDeclareStrategy
            .Setup(x => x.DeclareExchangeAsync("events/clients", ExchangeType.Topic, cancellationTokenSource.Token))
            .ReturnsAsync(exchange);
        deliveryModeStrategy
            .Setup(x => x.GetDeliveryMode(typeof(ClientCreatedEvent)))
            .Returns(2);
        advancedBus
            .Setup(x => x.PublishAsync(
                exchange.Name,
                "client-created-event",
                false,
                true,
                It.IsAny<EasyNetQ.IMessage>(),
                cancellationTokenSource.Token))
            .Callback<string, string, bool?, bool?, EasyNetQ.IMessage, CancellationToken>(
                (_, _, _, _, message, _) => publishedMessage = message)
            .Returns(Task.CompletedTask);

        await transport.DispatchAsync(@event, metadata, cancellationTokenSource.Token);

        Assert.NotNull(publishedMessage);
        Assert.Same(@event, publishedMessage.GetBody());
        Assert.Equal(typeof(ClientCreatedEvent), publishedMessage.MessageType);
        Assert.Equal(messageId.ToString("D"), publishedMessage.Properties.MessageId);
        Assert.Equal(correlationId.ToString("D"), publishedMessage.Properties.CorrelationId);
        Assert.Equal(createdAt.ToUnixTimeSeconds(), publishedMessage.Properties.Timestamp);
        Assert.Equal((byte)2, publishedMessage.Properties.DeliveryMode);
        exchangeDeclareStrategy.VerifyAll();
        deliveryModeStrategy.VerifyAll();
        advancedBus.VerifyAll();
    }

    [Fact]
    public async Task DispatchAsync_ForCommand_PublishesAsMandatoryAndUsesMessageIdAsCorrelationFallback()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy = new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var deliveryModeStrategy = new Mock<IMessageDeliveryModeStrategy>(MockBehavior.Strict);
        var transport = CreateTransport(advancedBus, exchangeDeclareStrategy, deliveryModeStrategy);
        var command = new StartLongWorkJobCommand(Guid.NewGuid());
        var messageId = Guid.NewGuid();
        var metadata = new MessageMetadata(messageId, DateTimeOffset.UtcNow);
        var exchange = CreateExchange("commands/long-work-job");
        EasyNetQ.IMessage? publishedMessage = null;
        exchangeDeclareStrategy
            .Setup(x => x.DeclareExchangeAsync("commands/long-work-job", ExchangeType.Topic, CancellationToken.None))
            .ReturnsAsync(exchange);
        deliveryModeStrategy
            .Setup(x => x.GetDeliveryMode(typeof(StartLongWorkJobCommand)))
            .Returns(2);
        advancedBus
            .Setup(x => x.PublishAsync(
                exchange.Name,
                "start-long-work-job-command",
                true,
                true,
                It.IsAny<EasyNetQ.IMessage>(),
                CancellationToken.None))
            .Callback<string, string, bool?, bool?, EasyNetQ.IMessage, CancellationToken>(
                (_, _, _, _, message, _) => publishedMessage = message)
            .Returns(Task.CompletedTask);

        await transport.DispatchAsync(command, metadata);

        Assert.NotNull(publishedMessage);
        Assert.Equal(messageId.ToString("D"), publishedMessage.Properties.CorrelationId);
        exchangeDeclareStrategy.VerifyAll();
        deliveryModeStrategy.VerifyAll();
        advancedBus.VerifyAll();
    }

    [Fact]
    public async Task DispatchAsync_WhenCancellationIsAlreadyRequested_DoesNotCallEasyNetQ()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy = new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var deliveryModeStrategy = new Mock<IMessageDeliveryModeStrategy>(MockBehavior.Strict);
        var transport = CreateTransport(advancedBus, exchangeDeclareStrategy, deliveryModeStrategy);
        var metadata = new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow);
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => transport.DispatchAsync(
                new ClientCreatedEvent("Acme"),
                metadata,
                cancellationTokenSource.Token));

        exchangeDeclareStrategy.VerifyNoOtherCalls();
        deliveryModeStrategy.VerifyNoOtherCalls();
        advancedBus.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task DispatchAsync_WithNullMessage_ThrowsArgumentNullException()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy = new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var deliveryModeStrategy = new Mock<IMessageDeliveryModeStrategy>(MockBehavior.Strict);
        var transport = CreateTransport(advancedBus, exchangeDeclareStrategy, deliveryModeStrategy);
        var metadata = new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow);

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => transport.DispatchAsync(null!, metadata));
    }

    [Fact]
    public async Task DispatchAsync_WithNullMetadata_ThrowsArgumentNullException()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy = new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var deliveryModeStrategy = new Mock<IMessageDeliveryModeStrategy>(MockBehavior.Strict);
        var transport = CreateTransport(advancedBus, exchangeDeclareStrategy, deliveryModeStrategy);

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => transport.DispatchAsync(new ClientCreatedEvent("Acme"), null!));
    }

    private static EasyNetQOutboundTransport CreateTransport(Mock<IAdvancedBus> advancedBus, Mock<IExchangeDeclareStrategy> exchangeDeclareStrategy, Mock<IMessageDeliveryModeStrategy> deliveryModeStrategy)
    {
        return new EasyNetQOutboundTransport(
            advancedBus.Object,
            exchangeDeclareStrategy.Object,
            deliveryModeStrategy.Object,
            new RabbitMqRouteResolver());
    }

    private static Exchange CreateExchange(string name)
    {
        return new Exchange(name, ExchangeType.Topic, true, false, new Dictionary<string, object>());
    }
}
