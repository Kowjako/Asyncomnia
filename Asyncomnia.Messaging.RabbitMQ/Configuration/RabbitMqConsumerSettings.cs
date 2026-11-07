namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

internal sealed record RabbitMqConsumerSettings(int ConsumerCount, ushort PrefetchCount, ushort DispatcherConcurrency);