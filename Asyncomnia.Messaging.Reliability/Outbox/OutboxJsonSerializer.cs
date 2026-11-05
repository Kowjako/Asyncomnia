using System.Text.Json;

namespace Asyncomnia.Messaging.Reliability.Outbox;

internal sealed class OutboxJsonSerializer
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public string Serialize(object message, Type messageType)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(messageType);

        return JsonSerializer.Serialize(message, messageType, Options);
    }

    public IMessage Deserialize(string messageJson, Type messageType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageJson);
        ArgumentNullException.ThrowIfNull(messageType);

        if (!typeof(IMessage).IsAssignableFrom(messageType))
        {
            throw new InvalidOperationException($"Type '{messageType}' does not implement {nameof(IMessage)}");
        }

        return JsonSerializer.Deserialize(messageJson, messageType, Options) as IMessage
            ?? throw new InvalidOperationException($"Could not deserialize message type '{messageType}'");
    }
}