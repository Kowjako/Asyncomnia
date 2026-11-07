namespace Asyncomnia.Messaging;

public sealed record MessageMetadata
{
    public Guid MessageId { get; }
    public Guid? CorrelationId { get; init; }
    public DateTimeOffset CreatedAt { get; }

    public MessageMetadata(Guid messageId, DateTimeOffset createdAt)
    {
        if (messageId == Guid.Empty)
        {
            throw new ArgumentException("Message ID cannot be empty", nameof(messageId));
        }

        MessageId = messageId;
        CreatedAt = createdAt;
    }
}