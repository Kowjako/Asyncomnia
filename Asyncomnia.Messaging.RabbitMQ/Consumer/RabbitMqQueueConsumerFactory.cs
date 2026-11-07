using Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;
using EasyNetQ;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal interface IRabbitMqQueueConsumerFactory
{
    Task<IAsyncDisposable> StartAsync(RabbitMqDeclaredSubscription subscription, CancellationToken ct = default);
}

internal sealed class RabbitMqQueueConsumerFactory(IAdvancedBus advancedBus, RabbitMqHandlerRegistrar handlerRegistrar) : IRabbitMqQueueConsumerFactory
{
    private readonly IAdvancedBus _advancedBus = advancedBus
        ?? throw new ArgumentNullException(nameof(advancedBus));

    private readonly RabbitMqHandlerRegistrar _handlerRegistrar = handlerRegistrar
        ?? throw new ArgumentNullException(nameof(handlerRegistrar));

    public Task<IAsyncDisposable> StartAsync(RabbitMqDeclaredSubscription subscription, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(subscription);
        ct.ThrowIfCancellationRequested();

        return _advancedBus.ConsumeAsync(configuration =>
        {
            configuration.ForQueue(subscription.Queue, registration =>
            {
                _handlerRegistrar.Register(registration, subscription.Subscription.Bindings);
            }, _ => { });
        });
    }
}