using Asyncomnia.Messaging.Internal;
using EasyNetQ;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Serialization;

internal static class RabbitMqMessagePropertiesMapper
{
    public const string JsonContentType = "application/json";
    public const string Utf8ContentEncoding = "utf-8";

    public static BasicProperties Map(Type messageType, MessageMetadata metadata)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(metadata);

        if (!typeof(IMessage).IsAssignableFrom(messageType))
        {
            throw new InvalidOperationException($"Type '{messageType}' does not implement {nameof(IMessage)}");
        }

        return new BasicProperties
        {
            ContentType = JsonContentType,
            ContentEncoding = Utf8ContentEncoding,
            Type = ClrMessageTypeResolver.GetContractType(messageType),
            MessageId = metadata.MessageId.ToString("D"),
            CorrelationId = (metadata.CorrelationId ?? metadata.MessageId).ToString("D"),
            Timestamp = new AmqpTimestamp(metadata.CreatedAt.ToUnixTimeSeconds()),
            DeliveryMode = DeliveryModes.Persistent
        };
    }

    // Tymczasowy adapter dla obecnego consumera EasyNetQ.
    // Zmienimy parametr na IReadOnlyBasicProperties podczas
    // migracji consumera do RabbitMQ.Client.
    public static MessageMetadata Map(
       in MessageProperties properties)
    {
        if (!Guid.TryParse(
                properties.MessageId,
                out var messageId) ||
            messageId == Guid.Empty)
        {
            throw new InvalidOperationException(
                $"RabbitMQ message has invalid MessageId: " +
                $"'{properties.MessageId}'");
        }

        Guid? correlationId = null;

        if (!string.IsNullOrWhiteSpace(
                properties.CorrelationId))
        {
            if (!Guid.TryParse(
                    properties.CorrelationId,
                    out var parsedCorrelationId) ||
                parsedCorrelationId == Guid.Empty)
            {
                throw new InvalidOperationException(
                    $"RabbitMQ message has invalid CorrelationId: " +
                    $"'{properties.CorrelationId}'");
            }

            correlationId = parsedCorrelationId;
        }

        DateTimeOffset createdAt;

        try
        {
            createdAt = DateTimeOffset.FromUnixTimeSeconds(
                properties.Timestamp);
        }
        catch (ArgumentOutOfRangeException exception)
        {
            throw new InvalidOperationException(
                $"RabbitMQ message has invalid Timestamp: " +
                $"'{properties.Timestamp}'",
                exception);
        }

        return new MessageMetadata(messageId, createdAt)
        {
            CorrelationId = correlationId
        };
    }
}