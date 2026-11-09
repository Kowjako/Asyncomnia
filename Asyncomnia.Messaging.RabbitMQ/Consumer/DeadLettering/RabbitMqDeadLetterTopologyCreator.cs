using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;

internal interface IRabbitMqDeadLetterTopologyCreator
{
    Task DeclareAsync(IReadOnlyCollection<RabbitMqDeclaredSubscription> subscriptions, CancellationToken token = default);
}

internal sealed class RabbitMqDeadLetterTopologyCreator(IRabbitMqChannelFactory channelFactory,
    RabbitMqDeadLetterRouteResolver routeResolver,
    IOptions<RabbitMqConsumerOptions> options) : IRabbitMqDeadLetterTopologyCreator
{
    private readonly IRabbitMqChannelFactory _channelFactory = channelFactory
        ?? throw new ArgumentNullException(nameof(channelFactory));

    private readonly RabbitMqDeadLetterRouteResolver _routeResolver = routeResolver
        ?? throw new ArgumentNullException(nameof(routeResolver));

    private readonly RabbitMqConsumerOptions _options = options?.Value
        ?? throw new ArgumentNullException(nameof(options));

    public async Task DeclareAsync(IReadOnlyCollection<RabbitMqDeclaredSubscription> subscriptions, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(subscriptions);

        if (!_options.DeadLetteringEnabled || subscriptions.Count == 0)
        {
            return;
        }

        var routes = subscriptions
            .Select(x => _routeResolver.Resolve(x.Subscription.QueueName))
            .DistinctBy(x => x.QueueName, StringComparer.Ordinal)
            .ToArray();

        await using var channel = await _channelFactory.CreateAsync(token: token);

        await channel.ExchangeDeclareAsync(
            exchange: routes[0].ExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null,
            passive: false,
            noWait: false,
            cancellationToken: token);

        foreach (var route in routes)
        {
            await channel.QueueDeclareAsync(
                queue: route.QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null,
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