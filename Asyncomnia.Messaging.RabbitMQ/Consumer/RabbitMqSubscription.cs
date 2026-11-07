namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal sealed record RabbitMqSubscription
{
    public string ExchangeName { get; init; } = null!;

    public string QueueName { get; init; } = null!;

    public IReadOnlyCollection<RabbitMqBinding> Bindings { get; set; } = [];
}