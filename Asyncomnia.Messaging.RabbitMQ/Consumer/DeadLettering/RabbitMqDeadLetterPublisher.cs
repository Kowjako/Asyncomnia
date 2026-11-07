using EasyNetQ;
using EasyNetQ.Consumer;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;

internal interface IRabbitMqDeadLetterPublisher
{
    Task PublishAsync(ConsumeContext context, CancellationToken token = default);
}

internal sealed class RabbitMqDeadLetterPublisher(IAdvancedBus advancedBus, IExchangeDeclareStrategy exchangeDeclareStrategy,
    RabbitMqDeadLetterRouteResolver routeResolver) : IRabbitMqDeadLetterPublisher
{
    private readonly IAdvancedBus _advancedBus = advancedBus
        ?? throw new ArgumentNullException(nameof(advancedBus));

    private readonly IExchangeDeclareStrategy _exchangeDeclareStrategy = exchangeDeclareStrategy
        ?? throw new ArgumentNullException(nameof(exchangeDeclareStrategy));

    private readonly RabbitMqDeadLetterRouteResolver _routeResolver = routeResolver
        ?? throw new ArgumentNullException(nameof(routeResolver));

    public async Task PublishAsync(ConsumeContext context, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();

        var route = _routeResolver.Resolve(context.ReceivedInfo.Queue);

        var exchange = await _exchangeDeclareStrategy.DeclareExchangeAsync(route.ExchangeName, ExchangeType.Topic, token);

        var queue = await _advancedBus.QueueDeclareAsync(
            route.QueueName,
            configuration =>
            {
                configuration.AsDurable(true);
                configuration.AsAutoDelete(false);
                configuration.AsExclusive(false);
            }, token);

        await _advancedBus.QueueBindAsync(queue.Name, exchange.Name, route.RoutingKey, cancellationToken: token);

        var properties = context.Properties with
        {
            Expiration = null // remove per-message TTL so after publication to DLQ it will not disappear :D 
        };

        await _advancedBus.PublishAsync(
            exchange: exchange.Name,
            routingKey: route.RoutingKey,
            mandatory: true,
            publisherConfirms: true,
            properties: properties,
            body: context.Body,
            cancellationToken: token);
    }
}
