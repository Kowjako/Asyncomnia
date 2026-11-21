namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

internal sealed class RabbitMqOptions
{
    /// <summary>
    /// AMQP connection URI, for example: amqp://guest:guest@localhost:5672/
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Maximum number of RabbitMQ channels in pool, that can publish messages
    /// concurrently within this application instance.
    /// 
    /// Because at the same time we can publish from application 
    /// logic as well as Outbox worker can publish and we need
    /// to guarantee that only one thread is publishing via specific channel (because channel is not thread-safe)
    /// So we are preparing N channels in pool for publishing
    /// </summary>
    public int PublisherChannelCount { get; set; } = 4;

    /// <summary>
    /// Captures events that cannot be routed to any queue.
    /// Commands are not affected and continue to use mandatory publishing.
    ///
    /// All applications sharing the same RabbitMQ virtual host must use
    /// the same value for exchanges with the same names.
    /// </summary>
    public bool CaptureUnroutableEvents { get; set; } = true;

    /// <summary>
    /// Maximum number of ready messages retained in the global unroutable queue.
    /// All applications sharing the same RabbitMQ virtual host must use the same value.
    /// </summary>
    public int UnroutableQueueMaxLength { get; set; } = 10_000;

    /// <summary>
    /// Maximum total body size, in bytes, retained in the global unroutable queue.
    /// All applications sharing the same RabbitMQ virtual host must use the same value.
    /// </summary>
    public long UnroutableQueueMaxLengthBytes { get; set; } = 256L * 1024 * 1024;

    public RabbitMqQueueType DefaultQueueType { get; set; } = RabbitMqQueueType.Quorum;
}
