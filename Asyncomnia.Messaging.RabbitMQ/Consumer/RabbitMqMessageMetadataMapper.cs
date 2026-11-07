using EasyNetQ;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

/// <summary>
/// This class is used when our Consumer receives message from RabbitMQ, to map AMQP properties
/// to our MessageMetadata class which will be used to construct MessageEnvelope which will be 
/// passed directly to IEventHandler / ICommandHandler implementation
/// </summary>
internal static class RabbitMqMessageMetadataMapper
{
    public static MessageMetadata Map(in MessageProperties props)
    {
        if (!Guid.TryParse(props.MessageId,out var messageId) || messageId == Guid.Empty)
        {
            throw new InvalidOperationException($"RabbitMQ message has invalid MessageId: '{props.MessageId}'");
        }

        Guid? correlationId = null;

        if (!string.IsNullOrWhiteSpace(props.CorrelationId))
        {
            if (!Guid.TryParse(props.CorrelationId, out var parsedCorrelationId) || parsedCorrelationId == Guid.Empty)
            {
                throw new InvalidOperationException($"RabbitMQ message has invalid CorrelationId: '{props.CorrelationId}'");
            }

            correlationId = parsedCorrelationId;
        }

        DateTimeOffset createdAt;

        try
        {
            createdAt = DateTimeOffset.FromUnixTimeSeconds(props.Timestamp);
        }
        catch (ArgumentOutOfRangeException exception)
        {
            throw new InvalidOperationException($"RabbitMQ message has invalid Timestamp: '{props.Timestamp}'", exception);
        }

        return new MessageMetadata(messageId, createdAt)
        {
            CorrelationId = correlationId
        };
    }
}
