using EasyNetQ;
using EasyNetQ.Consumer;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;

internal interface IRabbitMqDeadLetterPublisher
{
    Task PublishAsync(RabbitMqConsumeContext context, CancellationToken token = default);
}

internal sealed class RabbitMqDeadLetterPublisher(IAdvancedBus advancedBus, RabbitMqDeadLetterRouteResolver routeResolver) 
    : IRabbitMqDeadLetterPublisher
{
    private readonly IAdvancedBus _advancedBus = advancedBus
        ?? throw new ArgumentNullException(nameof(advancedBus));

    private readonly RabbitMqDeadLetterRouteResolver _routeResolver = routeResolver
        ?? throw new ArgumentNullException(nameof(routeResolver));

    public async Task PublishAsync(RabbitMqConsumeContext context, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();

        var route = _routeResolver.Resolve(context.ReceivedInfo.Queue);

        var properties = context.Properties with
        {
            Expiration = null // remove per-message TTL so after publication to DLQ it will not disappear :D 
        };

        await _advancedBus.PublishAsync(
            exchange: route.ExchangeName,
            routingKey: route.RoutingKey,
            mandatory: true,
            publisherConfirms: true,
            properties: properties,
            body: context.Body,
            cancellationToken: token);
    }
}
