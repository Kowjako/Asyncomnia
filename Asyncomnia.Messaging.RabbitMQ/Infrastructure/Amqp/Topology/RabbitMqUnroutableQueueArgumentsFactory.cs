using Asyncomnia.Messaging.RabbitMQ.Configuration;

namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;

internal static class RabbitMqUnroutableQueueArgumentsFactory
{
    public static IDictionary<string, object?> Create(RabbitMqOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new Dictionary<string, object?>
        {
            ["x-max-length"] = options.UnroutableQueueMaxLength,
            ["x-max-length-bytes"] = options.UnroutableQueueMaxLengthBytes,
            ["x-overflow"] = "drop-head"
        };
    }
}