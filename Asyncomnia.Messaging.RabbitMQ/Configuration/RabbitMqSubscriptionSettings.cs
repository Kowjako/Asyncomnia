namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

internal sealed record RabbitMqSubscriptionSettings(
    RabbitMqQueueType QueueType,
    bool SingleActiveConsumer,
    ushort PrefetchCount,
    ushort DispatcherConcurrency);
