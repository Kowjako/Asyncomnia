namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal sealed record RabbitMqBinding(string RoutingKey, Type MessageType);