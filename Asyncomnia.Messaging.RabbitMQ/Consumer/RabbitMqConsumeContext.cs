using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal sealed record RabbitMqConsumeContext(
    string QueueName,
    string ExchangeName,
    string RoutingKey,
    IReadOnlyBasicProperties Properties,
    ReadOnlyMemory<byte> Body);