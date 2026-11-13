namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

internal sealed record RabbitMqSubscriptionSettings(bool SingleActiveConsumer, ushort PrefetchCount, ushort DispatcherConcurrency);