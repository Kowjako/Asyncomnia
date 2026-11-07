namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

public sealed class RabbitMqSubscriptionConsumerOptions
{
    /// <summary>
    /// Number of independent RabbitMQ consumers(channels)
    /// created for the subscription queue.
    /// </summary>
    public int? ConsumerCount { get; set; }

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
