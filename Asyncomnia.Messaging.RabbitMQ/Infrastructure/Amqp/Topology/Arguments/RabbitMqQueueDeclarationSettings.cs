using Asyncomnia.Messaging.RabbitMQ.Configuration;

namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology.Arguments;

internal sealed record RabbitMqQueueDeclarationSettings(
    RabbitMqQueueType QueueType,
    bool SingleActiveConsumer = false,
    TimeSpan? MessageTtl = null,
    string? DeadLetterExchange = null,
    int? MaxLength = null,
    long? MaxLengthBytes = null,
    RabbitMqQueueOverflowStrategy? Overflow = null);