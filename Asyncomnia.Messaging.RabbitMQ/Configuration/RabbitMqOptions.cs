namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

public sealed class RabbitMqOptions
{
    /// <summary>
    /// AMQP connection URI, for example: amqp://guest:guest@localhost:5672/
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;
}