namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;

internal static class RabbitMqUnroutableDefaults
{
    public const string ExchangeName = "asyncomnia/unroutable";
    public const string QueueName = "asyncomnia/unroutable-messages";
}