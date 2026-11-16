namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

public sealed class RabbitMqOptions
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
}