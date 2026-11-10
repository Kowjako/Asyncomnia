using EasyNetQ;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal interface IRabbitMqQueueConsumerFactory
{
    Task<IAsyncDisposable> StartAsync(RabbitMqSubscription subscription, CancellationToken ct = default);
}

internal sealed class RabbitMqQueueConsumerFactory(IAdvancedBus advancedBus, RabbitMqHandlerRegistrar handlerRegistrar) : IRabbitMqQueueConsumerFactory
{
    private readonly IAdvancedBus _advancedBus = advancedBus
        ?? throw new ArgumentNullException(nameof(advancedBus));

    private readonly RabbitMqHandlerRegistrar _handlerRegistrar = handlerRegistrar
        ?? throw new ArgumentNullException(nameof(handlerRegistrar));

    public Task<IAsyncDisposable> StartAsync(RabbitMqSubscription subscription, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(subscription);
        ct.ThrowIfCancellationRequested();

        var settings = subscription.ConsumerSettings;

        return _advancedBus.ConsumeAsync(configuration =>
        {
            configuration.WithPrefetchCount(settings.PrefetchCount)
                         .WithConsumerDispatcherConcurrency(settings.DispatcherConcurrency);

            configuration.ForQueue(new EasyNetQ.Topology.Queue(subscription.QueueName), registration =>
            {
                _handlerRegistrar.Register(registration, subscription.Bindings);
            }, _ => { });
        });
    }
}