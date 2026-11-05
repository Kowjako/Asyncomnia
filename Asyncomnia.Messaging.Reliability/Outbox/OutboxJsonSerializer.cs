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
}
