using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Serialization;

internal sealed class RabbitMqMessagePropertiesFactory
{
    public static BasicProperties Create(Type messageType, MessageMetadata metadata)
    {
        // mapowanie właściwości
    }
}