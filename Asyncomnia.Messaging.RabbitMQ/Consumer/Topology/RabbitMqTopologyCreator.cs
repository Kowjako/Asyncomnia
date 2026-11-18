using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;

internal sealed class RabbitMqTopologyCreator(
    IRabbitMqChannelFactory channelFactory,
    RabbitMqSubscriptionResolver subscriptionResolver,
    IRabbitMqUnroutableTopologyCreator unroutableTopologyCreator)
{
    private readonly IRabbitMqChannelFactory _channelFactory = channelFactory
        ?? throw new ArgumentNullException(nameof(channelFactory));

    private readonly RabbitMqSubscriptionResolver _subscriptionResolver = subscriptionResolver
        ?? throw new ArgumentNullException(nameof(subscriptionResolver));

    private readonly IRabbitMqUnroutableTopologyCreator _unroutableTopologyCreator = unroutableTopologyCreator
        ?? throw new ArgumentNullException(nameof(unroutableTopologyCreator));

    public async Task<IReadOnlyCollection<RabbitMqSubscription>> DeclareAsync(CancellationToken token = default)
    {
        var subscriptions = _subscriptionResolver.Resolve();
        if (subscriptions.Count == 0)
        {
            return subscriptions;
        }

        token.ThrowIfCancellationRequested();

        if (subscriptions.Any(x => x.UseUnroutableFallback))
        {
            await _unroutableTopologyCreator.EnsureDeclaredAsync(token);
        }

        await using var channel = await _channelFactory.CreateAsync(token: token);

        foreach (var sub in subscriptions)
        {
            token.ThrowIfCancellationRequested();

            // Publisher and consumer can both declare the same exchange. Their definitions,
            // including alternate-exchange arguments, must be identical; otherwise RabbitMQ
            // closes the channel with PRECONDITION_FAILED.
            await channel.ExchangeDeclareAsync(
                exchange: sub.ExchangeName,
                ExchangeType.Topic,
                durable: true,
                autoDelete: false,
                arguments: RabbitMqExchangeArgumentsFactory.Create(sub.UseUnroutableFallback),
                passive: false,
                noWait: false,
                cancellationToken: token);

            await channel.QueueDeclareAsync(
                queue: sub.QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: CreateQueueArguments(sub),
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

    private static IDictionary<string, object?>? CreateQueueArguments(RabbitMqSubscription subscription)
    {
        if (!subscription.Settings.SingleActiveConsumer)
        {
            return null;
        }

        return new Dictionary<string, object?>
        {
            ["x-single-active-consumer"] = true
        };
    }
}
