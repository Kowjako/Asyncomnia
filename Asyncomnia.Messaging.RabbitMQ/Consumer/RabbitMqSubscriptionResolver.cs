using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Routing;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal sealed class RabbitMqSubscriptionResolver(
    IMessageHandlerCatalog catalog,
    MessageRouteResolver routeResolver,
    IOptions<RabbitMqConsumerOptions> options,
    IOptions<RabbitMqOptions> rabbitMqOptions)
{
    private readonly IMessageHandlerCatalog _handlerCatalog = catalog
        ?? throw new ArgumentNullException(nameof(catalog));

    private readonly MessageRouteResolver _routeResolver = routeResolver
        ?? throw new ArgumentNullException(nameof(routeResolver));

    private readonly RabbitMqConsumerOptions _options = options?.Value
        ?? throw new ArgumentNullException(nameof(options));

    private readonly RabbitMqOptions _rabbitMqOptions = rabbitMqOptions?.Value
        ?? throw new ArgumentNullException(nameof(rabbitMqOptions));

    public IReadOnlyCollection<RabbitMqSubscription> Resolve()
    {
        var routedMessages = _handlerCatalog.EventTypes
            .Concat(_handlerCatalog.CommandTypes)
            .Select(mt => (MessageType: mt, Route: _routeResolver.Resolve(mt)))
            .ToArray();

        return routedMessages.GroupBy(x => x.Route.ExchangeName, StringComparer.Ordinal)
                             .Select(g => new RabbitMqSubscription
                             {
                                 ExchangeName = g.Key,
                                 QueueName = $"{g.Key}/subscription/{_options.EndpointName}",
                                 Bindings = g.Select(x => new RabbitMqBinding(x.Route.RoutingKey, x.MessageType))
                                             .OrderBy(x => x.RoutingKey, StringComparer.Ordinal)
                                             .ToArray(),
                                 Settings = _options.GetSubscriptionSettings(g.Key),
                                 UseUnroutableFallback =
                                     _rabbitMqOptions.CaptureUnroutableEvents &&
                                     g.All(x => typeof(IEvent).IsAssignableFrom(x.MessageType))
                             })
                             .OrderBy(x => x.ExchangeName, StringComparer.Ordinal)
                             .ToArray();
    }
}
