using Asyncomnia.Messaging.RabbitMQ.Configuration;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal sealed record RabbitMqSubscription
{
    public string ExchangeName { get; init; } = null!;

    public string QueueName { get; init; } = null!;

    public IReadOnlyCollection<RabbitMqBinding> Bindings { get; init; } = [];

    public RabbitMqConsumerSettings ConsumerSettings { get; init; } = null!;
}