namespace Asyncomnia.Messaging.Reliability.Inbox;

public sealed record InboxMessage
{
    public Guid MessageId { get; }

    public string ConsumerId { get; }

    public string ContractType { get; }

    public DateTimeOffset ReceivedAt { get; }

    public InboxMessage(Guid messageId, string consumerId, string contractType, DateTimeOffset receivedAt)
    {
        if (messageId == Guid.Empty)
        {
            throw new ArgumentException("Message ID cannot be empty", nameof(messageId));
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(consumerId);
        ArgumentException.ThrowIfNullOrWhiteSpace(contractType);

        MessageId = messageId;
        ConsumerId = consumerId;
        ContractType = contractType;
        ReceivedAt = receivedAt;
    }
}