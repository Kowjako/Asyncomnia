using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology.Arguments;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.Unhandled;

internal interface IRabbitMqUnhandledTopologyCreator
{
    Task DeclareAsync(IReadOnlyCollection<RabbitMqSubscription> subscriptions, CancellationToken token = default);
}

internal sealed class RabbitMqUnhandledTopologyCreator(
    IRabbitMqChannelFactory channelFactory,
    RabbitMqUnhandledRouteResolver routeResolver,
    IOptions<RabbitMqConsumerOptions> options) : IRabbitMqUnhandledTopologyCreator
{
    private readonly IRabbitMqChannelFactory _channelFactory = channelFactory 
        ?? throw new ArgumentNullException(nameof(channelFactory));

    private readonly RabbitMqUnhandledRouteResolver _routeResolver = routeResolver
        ?? throw new ArgumentNullException(nameof(routeResolver));

    private readonly RabbitMqConsumerOptions _options = options?.Value 
        ?? throw new ArgumentNullException(nameof(options));

    public async Task DeclareAsync(IReadOnlyCollection<RabbitMqSubscription> subscriptions, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(subscriptions);

        var routes = subscriptions
            .Select(subscription => _routeResolver.Resolve(subscription.QueueName))
            .DistinctBy(route => route.QueueName, StringComparer.Ordinal)
            .ToList();

        if (routes.Count == 0)
        {
            return;
        }

        token.ThrowIfCancellationRequested();

        await using var channel = await _channelFactory.CreateAsync(token: token);

        await channel.ExchangeDeclareAsync(
            exchange: routes[0].ExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null,
            noWait: false,
            cancellationToken: token);

        foreach (var route in routes)
        {
            await channel.QueueDeclareAsync(
                queue: route.QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: RabbitMqQueueArgumentsFactory.Create(new RabbitMqQueueDeclarationSettings(
                    QueueType: _options.DefaultQueueType)),
                passive: false,
                noWait: false,
                cancellationToken: token);

            await channel.QueueBindAsync(
                queue: route.QueueName,
                exchange: route.ExchangeName,
                routingKey: route.RoutingKey,
                arguments: null,
                noWait: false,
                cancellationToken: token);
        }
    }
}
