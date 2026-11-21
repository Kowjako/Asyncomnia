using Asyncomnia.Messaging.RabbitMQ.Configuration;

namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology.Arguments;

internal static class RabbitMqQueueArgumentsFactory
{
    public static IDictionary<string, object?> Create(RabbitMqQueueDeclarationSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);

        var arguments = new Dictionary<string, object?>
        {
            ["x-queue-type"] = settings.QueueType switch
            {
                RabbitMqQueueType.Classic => "classic",
                RabbitMqQueueType.Quorum => "quorum",
                _ => throw new ArgumentOutOfRangeException(nameof(settings.QueueType))
            }
        };

        if (settings.SingleActiveConsumer)
        {
            arguments["x-single-active-consumer"] = true;
        }

        if (settings.MessageTtl.HasValue)
        {
            arguments["x-message-ttl"] = checked((long)settings.MessageTtl.Value.TotalMilliseconds);
        }

        if (settings.DeadLetterExchange is not null)
        {
            arguments["x-dead-letter-exchange"] = settings.DeadLetterExchange;
        }

        if (settings.MaxLength.HasValue)
        {
            arguments["x-max-length"] = settings.MaxLength.Value;
        }

        if (settings.MaxLengthBytes.HasValue)
        {
            arguments["x-max-length-bytes"] = settings.MaxLengthBytes.Value;
        }

        if (settings.Overflow.HasValue)
        {
            arguments["x-overflow"] = settings.Overflow switch
            {
                RabbitMqQueueOverflowStrategy.DropHead => "drop-head",
                RabbitMqQueueOverflowStrategy.RejectPublish => "reject-publish",
                _ => throw new ArgumentOutOfRangeException(nameof(settings.Overflow))
            };
        }

        return arguments;
    }
}