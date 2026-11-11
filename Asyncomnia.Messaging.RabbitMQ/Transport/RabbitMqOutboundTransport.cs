using Asyncomnia.Messaging.Internal.Serialization;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Serialization;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;
using Asyncomnia.Messaging.RabbitMQ.Routing;
using Asyncomnia.Messaging.Transport;

namespace Asyncomnia.Messaging.RabbitMQ.Transport;

internal sealed class RabbitMqOutboundTransport : IOutboundTransport
{
    private readonly MessageRouteResolver _msgRouteResolver;
    private readonly MessageJsonSerializer _msgSerializer;
    private readonly IRabbitMqExchangeCreator _exchangeCreator;
    private readonly IRabbitMqPublisher _rabbitMqPublisher;

    public RabbitMqOutboundTransport(MessageRouteResolver msgRouteResolver, MessageJsonSerializer msgSerializer,
        IRabbitMqExchangeCreator exchangeCreator, IRabbitMqPublisher rabbitMqPublisher)
    {
        _msgRouteResolver = msgRouteResolver ?? throw new ArgumentNullException(nameof(msgRouteResolver));
        _msgSerializer = msgSerializer ?? throw new ArgumentNullException(nameof(msgSerializer));
        _exchangeCreator = exchangeCreator ?? throw new ArgumentNullException(nameof(exchangeCreator));
        _rabbitMqPublisher = rabbitMqPublisher ?? throw new ArgumentNullException(nameof(rabbitMqPublisher));
    }

    public async Task DispatchAsync(IMessage message, MessageMetadata metadata, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();

        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(metadata);

        var messageType = message.GetType();
        var route = _msgRouteResolver.Resolve(messageType);

        var body = _msgSerializer.SerializeToUtf8Bytes(message);
        var properties = RabbitMqMessagePropertiesMapper.MapToBrokerProperties(messageType, metadata);

        await _exchangeCreator.EnsureDeclaredAsync(route.ExchangeName, token);
        await _rabbitMqPublisher.PublishAsync(
            exchange: route.ExchangeName,
            routingKey: route.RoutingKey,
            mandatory: message is ICommand,
            properties,
            body,
            token);
    }
}