namespace Asyncomnia.Messaging.RabbitMQ.Transport;

internal static class RabbitMqHeaders
{
    public const string MessageId = "asyncomnia-message-id";
    public const string CorrelationId = "asyncomnia-correlation-id";
    public const string CreatedAt = "asyncomnia-created-at";

    public static IDictionary<string, object> Create(MessageMetadata metadata)
    {
        var headers = new Dictionary<string, object>
        {
            [MessageId] = metadata.MessageId.ToString("D"),
            [CreatedAt] = metadata.CreatedAt.ToString("O")
        };

        if (metadata.CorrelationId.HasValue)
        {
            headers[CorrelationId] = metadata.CorrelationId.Value.ToString("D");
        }

        return headers;
    }
}