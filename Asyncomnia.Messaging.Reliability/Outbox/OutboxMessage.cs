using System;
using System.Collections.Generic;
using System.Text;

namespace Asyncomnia.Messaging.Reliability.Outbox;

public sealed record OutboxMessage
{
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

    public Guid MessageId { get; init; }
    public string ContractType { get; init; }
    public string MessageJson { get; init; }
    public Guid? CorrelationId { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
}