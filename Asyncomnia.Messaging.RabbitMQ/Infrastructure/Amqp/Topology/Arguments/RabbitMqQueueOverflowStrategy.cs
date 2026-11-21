namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology.Arguments;

internal enum RabbitMqQueueOverflowStrategy
{
    DropHead,
    RejectPublish
}