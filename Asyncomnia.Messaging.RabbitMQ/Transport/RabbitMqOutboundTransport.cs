using Asyncomnia.Messaging.Internal.Serialization;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Serialization;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;
using Asyncomnia.Messaging.RabbitMQ.Routing;
using Asyncomnia.Messaging.Transport;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.Transport;

internal sealed class RabbitMqOutboundTransport : IOutboundTransport
{
    private readonly MessageRouteResolver _msgRouteResolver;
    private readonly MessageJsonSerializer _msgSerializer;
    private readonly IRabbitMqExchangeCreator _exchangeCreator;
    private readonly IRabbitMqPublisher _rabbitMqPublisher;
    private readonly RabbitMqOptions _rabbitMqOptions;

    public RabbitMqOutboundTransport(
        MessageRouteResolver msgRouteResolver,
        MessageJsonSerializer msgSerializer,
        IRabbitMqExchangeCreator exchangeCreator,
        IRabbitMqPublisher rabbitMqPublisher,
        IOptions<RabbitMqOptions> options)
    {
        _msgRouteResolver = msgRouteResolver ?? throw new ArgumentNullException(nameof(msgRouteResolver));
        _msgSerializer = msgSerializer ?? throw new ArgumentNullException(nameof(msgSerializer));
        _exchangeCreator = exchangeCreator ?? throw new ArgumentNullException(nameof(exchangeCreator));
        _rabbitMqPublisher = rabbitMqPublisher ?? throw new ArgumentNullException(nameof(rabbitMqPublisher));
        _rabbitMqOptions = options?.Value ?? throw new ArgumentNullException(nameof(options));
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

        var useUnroutableFallback = message is IEvent && _rabbitMqOptions.CaptureUnroutableEvents;
        await _exchangeCreator.EnsureDeclaredAsync(route.ExchangeName, useUnroutableFallback, token);

        await _rabbitMqPublisher.PublishAsync(
            exchange: route.ExchangeName,
            routingKey: route.RoutingKey,
            mandatory: message is ICommand,
            properties,
            body,
            token);
    }
}
