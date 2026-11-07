using Asyncomnia.Messaging.RabbitMQ.Routing;
using Asyncomnia.Messaging.Transport;
using EasyNetQ;

namespace Asyncomnia.Messaging.RabbitMQ.Transport;

internal sealed class EasyNetQOutboundTransport(IAdvancedBus advancedBus,
    IExchangeDeclareStrategy exchangeDeclareStrategy, 
    IMessageDeliveryModeStrategy messageDeliveryModeStrategy,
    MessageRouteResolver routeResolver) : IOutboundTransport
{
    private readonly IAdvancedBus _advancedBus = advancedBus
        ?? throw new ArgumentNullException(nameof(advancedBus));

    private readonly IExchangeDeclareStrategy _exchangeDeclareStrategy = exchangeDeclareStrategy
        ?? throw new ArgumentNullException(nameof(exchangeDeclareStrategy));

    private readonly IMessageDeliveryModeStrategy _messageDeliveryModeStrategy = messageDeliveryModeStrategy
        ?? throw new ArgumentNullException(nameof(messageDeliveryModeStrategy));

    private readonly MessageRouteResolver _routeResolver = routeResolver
        ?? throw new ArgumentNullException(nameof(routeResolver));

    public async Task DispatchAsync(IMessage message, MessageMetadata metadata, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(metadata);
        token.ThrowIfCancellationRequested();

        var messageType = message.GetType();
        var route = _routeResolver.Resolve(messageType);

        var exchange = await _exchangeDeclareStrategy.DeclareExchangeAsync(route.ExchangeName, ExchangeType.Topic, token);

        var properties = new MessageProperties
        {
            MessageId = metadata.MessageId.ToString("D"),
            CorrelationId = (metadata.CorrelationId ?? metadata.MessageId).ToString("D"),
            Timestamp = metadata.CreatedAt.ToUnixTimeSeconds(),
            DeliveryMode = _messageDeliveryModeStrategy.GetDeliveryMode(messageType)
        };

        var brokerMessage = MessageFactory.CreateInstance(messageType, message, in properties);

        await _advancedBus.PublishAsync(
            exchange: exchange.Name,
            routingKey: route.RoutingKey,
            mandatory: message is ICommand,
            publisherConfirms: true,
            message: brokerMessage,
            cancellationToken: token);
    }
}