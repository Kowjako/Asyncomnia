using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;
using EasyNetQ;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;

internal interface IRabbitMqDeadLetterTopologyCreator
{
    Task DeclareAsync(IReadOnlyCollection<RabbitMqDeclaredSubscription> subscriptions, CancellationToken token = default);
}

internal sealed class RabbitMqDeadLetterTopologyCreator(IAdvancedBus advancedBus, IExchangeDeclareStrategy exchangeDeclareStrategy,
    RabbitMqDeadLetterRouteResolver routeResolver, IOptions<RabbitMqConsumerOptions> options) : IRabbitMqDeadLetterTopologyCreator
{
    private readonly RabbitMqConsumerOptions _options = options.Value;

    public async Task DeclareAsync(IReadOnlyCollection<RabbitMqDeclaredSubscription> subscriptions, CancellationToken token = default)
    {
        if (!_options.DeadLetteringEnabled || subscriptions.Count == 0)
        {
            return;
        }

        var routes = subscriptions
            .Select(x => routeResolver.Resolve(x.Subscription.QueueName))
            .DistinctBy(x => x.QueueName, StringComparer.Ordinal)
            .ToArray();

        var exchange = await exchangeDeclareStrategy.DeclareExchangeAsync(routes[0].ExchangeName, ExchangeType.Direct, token);

        foreach (var route in routes)
        {
            var queue = await advancedBus.QueueDeclareAsync(route.QueueName,
                configuration =>
                {
                    configuration.AsDurable(true);
                    configuration.AsAutoDelete(false);
                    configuration.AsExclusive(false);
                }, token);

            await advancedBus.QueueBindAsync(queue.Name, exchange.Name, route.RoutingKey, cancellationToken: token);
        }
    }
}