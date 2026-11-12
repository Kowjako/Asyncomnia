namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

public sealed class RabbitMqSubscriptionOptions
{
    /// <summary>
    /// Determines whether RabbitMQ should deliver messages to only one active
    /// consumer across all instances connected to the subscription queue.
    /// Standby consumers are promoted automatically when the active consumer disconnects.
    /// This does not limit concurrent processing within the active consumer.
    /// </summary>
    public bool? SingleActiveConsumer { get; set; }

    /// <summary>
    /// Maximum number of unacknowledged messages delivered
    /// to a single consumer(channel).
    /// </summary>
    public ushort? PrefetchCount { get; set; }

    /// <summary>
    /// Maximum number of messages processed concurrently
    /// by a single consumer.
    /// </summary>
    public ushort? DispatcherConcurrency { get; set; }
}
