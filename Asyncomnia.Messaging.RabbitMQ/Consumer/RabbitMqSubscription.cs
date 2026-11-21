using System.Collections.Frozen;
using Asyncomnia.Messaging.RabbitMQ.Configuration;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal sealed record RabbitMqSubscription
{
    public string ExchangeName { get; init; } = null!;

    public string QueueName { get; init; } = null!;

    // MessageType <-> RoutingKey frozen dictionary, built only during startup, then only reads in runtime
    // awesome performance in comparison with IReadOnlyList / LINQ / etc.
    public FrozenDictionary<Type, string> Bindings { get; init; } = FrozenDictionary<Type, string>.Empty;

    public RabbitMqSubscriptionSettings Settings { get; init; } = null!;

    public bool UseUnroutableFallback { get; init; }
}
