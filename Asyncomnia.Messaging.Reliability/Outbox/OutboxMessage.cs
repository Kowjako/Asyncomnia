namespace Asyncomnia.Messaging.Reliability.Outbox;

public sealed record OutboxMessage
{
    public Guid MessageId { get; }
    public string ContractType { get; }
    public string MessageJson { get; }
    public Guid? CorrelationId { get; }
    public DateTimeOffset CreatedAt { get; }

    public OutboxMessage(Guid messageId, string contractType, string messageJson, Guid? correlationId, DateTimeOffset createdAt)
    {
        if (messageId == Guid.Empty)
            throw new ArgumentException("Message ID cannot be empty", nameof(messageId));

        ArgumentException.ThrowIfNullOrWhiteSpace(contractType);
        ArgumentException.ThrowIfNullOrWhiteSpace(messageJson);

        if (correlationId == Guid.Empty)
            throw new ArgumentException("Correlation ID cannot be empty", nameof(correlationId));

        MessageId = messageId;
        ContractType = contractType;
        MessageJson = messageJson;
        CorrelationId = correlationId;
        CreatedAt = createdAt;
    }
}