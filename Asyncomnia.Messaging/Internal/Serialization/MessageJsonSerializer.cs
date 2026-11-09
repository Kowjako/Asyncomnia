using System.Text.Json;

namespace Asyncomnia.Messaging.Internal.Serialization;

internal sealed class MessageJsonSerializer
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public string SerializeToString(IMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return JsonSerializer.Serialize(message, message.GetType(), Options);
    }

    public byte[] SerializeToUtf8Bytes(IMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return JsonSerializer.SerializeToUtf8Bytes(message, message.GetType(), Options);
    }

    public IMessage Deserialize(string messageJson, Type messageType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageJson);
        ValidateMessageType(messageType);

        return JsonSerializer.Deserialize(messageJson, messageType, Options) as IMessage
            ?? throw CreateDeserializationException(messageType);
    }

    public IMessage Deserialize(ReadOnlyMemory<byte> body, Type messageType)
    {
        if (body.IsEmpty)
        {
            throw new ArgumentException("Message body cannot be empty", nameof(body));
        }

        ValidateMessageType(messageType);

        return JsonSerializer.Deserialize(body.Span, messageType, Options) as IMessage
            ?? throw CreateDeserializationException(messageType);
    }

    private static void ValidateMessageType(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        if (!typeof(IMessage).IsAssignableFrom(messageType))
        {
            throw new InvalidOperationException($"Type '{messageType}' does not implement {nameof(IMessage)}");
        }
    }

    private static InvalidOperationException CreateDeserializationException(Type messageType)
    {
        return new InvalidOperationException($"Could not deserialize message type '{messageType}'");
    }
}