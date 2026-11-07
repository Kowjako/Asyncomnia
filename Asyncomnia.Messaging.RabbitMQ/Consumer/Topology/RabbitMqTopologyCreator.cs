using EasyNetQ;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;

internal sealed class RabbitMqTopologyCreator(IAdvancedBus advancedBus, 
    IExchangeDeclareStrategy exchangeDeclareStrategy, RabbitMqSubscriptionResolver subscriptionResolver)
{
    private readonly IAdvancedBus _advancedBus = advancedBus
        ?? throw new ArgumentNullException(nameof(advancedBus));

    private readonly IExchangeDeclareStrategy _exchangeDeclareStrategy = exchangeDeclareStrategy
        ?? throw new ArgumentNullException(nameof(exchangeDeclareStrategy));

    private readonly RabbitMqSubscriptionResolver _subscriptionResolver = subscriptionResolver
        ?? throw new ArgumentNullException(nameof(subscriptionResolver));

    public async Task<IReadOnlyCollection<RabbitMqDeclaredSubscription>> DeclareAsync(CancellationToken token = default)
    {
        var subscriptions = _subscriptionResolver.Resolve();
        var declaredSubscriptions = new List<RabbitMqDeclaredSubscription>();

        foreach (var sub in subscriptions)
        {
            token.ThrowIfCancellationRequested();

            var exchange = await _exchangeDeclareStrategy.DeclareExchangeAsync(sub.ExchangeName, ExchangeType.Topic, token);
            var queue = await _advancedBus.QueueDeclareAsync(sub.QueueName, cfg =>
            {
                cfg.AsDurable(true);
                cfg.AsAutoDelete(false);
                cfg.AsExclusive(false);
            }, token);

            foreach (var binding in sub.Bindings)
            {
                await _advancedBus.QueueBindAsync(queue.Name, exchange.Name, binding.RoutingKey, cancellationToken: token);
            }

            declaredSubscriptions.Add(new RabbitMqDeclaredSubscription(sub, queue));
        }

        return declaredSubscriptions;
    }
}
