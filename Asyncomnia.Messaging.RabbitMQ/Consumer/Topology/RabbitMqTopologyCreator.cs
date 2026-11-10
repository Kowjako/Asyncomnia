using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;

internal sealed class RabbitMqTopologyCreator(IRabbitMqChannelFactory channelFactory,
    RabbitMqSubscriptionResolver subscriptionResolver)
{
    private readonly IRabbitMqChannelFactory _channelFactory = channelFactory
        ?? throw new ArgumentNullException(nameof(channelFactory));

    private readonly RabbitMqSubscriptionResolver _subscriptionResolver = subscriptionResolver
        ?? throw new ArgumentNullException(nameof(subscriptionResolver));

    public async Task<IReadOnlyCollection<RabbitMqSubscription>> DeclareAsync(CancellationToken token = default)
    {
        var subscriptions = _subscriptionResolver.Resolve();
        if (subscriptions.Count == 0)
        {
            return subscriptions;
        }

        token.ThrowIfCancellationRequested();

        await using var channel = await _channelFactory.CreateAsync(token: token);

        foreach (var sub in subscriptions)
        {
            token.ThrowIfCancellationRequested();

            await channel.ExchangeDeclareAsync(
                exchange: sub.ExchangeName,
                ExchangeType.Topic,
                durable: true,
                autoDelete: false,
                arguments: null,
                passive: false,
                noWait: false,
                cancellationToken: token);

            await channel.QueueDeclareAsync(
                queue: sub.QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null,
                noWait: false,
                cancellationToken: token);

            foreach (var binding in sub.Bindings)
            {
                await channel.QueueBindAsync(
                    queue: sub.QueueName,
                    exchange: sub.ExchangeName,
                    routingKey: binding.RoutingKey,
                    arguments: null,
                    noWait: false,
                    cancellationToken: token);
            }
        }

        return subscriptions;
    }
}
