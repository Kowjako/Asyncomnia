namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

public sealed class RabbitMqOptions
{
    /// <summary>
    /// AMQP connection URI, for example: amqp://guest:guest@localhost:5672/
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Maximum number of RabbitMQ channels that can publish messages
    /// concurrently within this application instance.
    /// 
    /// Because at the same time we can publish from application 
    /// logic as well as Outbox worker can publish at the same time.
    /// Also we can have for instance 4 outbox workers, then using 1 channel will be bottleneck
    /// </summary>
    public int PublisherChannelCount { get; set; } = 4;
}