namespace Asyncomnia.Messaging.Reliability.DelayedPublishing;

public sealed record DelayedMessage
{
    public Guid ScheduleId { get; }
    public Guid MessageId { get; }
    public string ContractType { get; }
    public string MessageJson { get; }
    public Guid? CorrelationId { get; }
    public DateTimeOffset CreatedAt { get; }
    public DateTimeOffset DeliverAt { get; }

    public DelayedMessage(Guid scheduleId, Guid messageId, string contractType,
        string messageJson, Guid? correlationId, DateTimeOffset createdAt, DateTimeOffset deliverAt)
    {
        if (scheduleId == Guid.Empty)
        {
            throw new ArgumentException("Schedule ID cannot be empty", nameof(scheduleId));
        }

        if (messageId == Guid.Empty)
        {
            throw new ArgumentException("Message ID cannot be empty", nameof(messageId));
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(contractType);
        ArgumentException.ThrowIfNullOrWhiteSpace(messageJson);

        if (correlationId == Guid.Empty)
        {
            throw new ArgumentException("Correlation ID cannot be empty", nameof(correlationId));
        }

        ScheduleId = scheduleId;
        MessageId = messageId;
        ContractType = contractType;
        MessageJson = messageJson;
        CorrelationId = correlationId;
        CreatedAt = createdAt;
        DeliverAt = deliverAt;
    }
}
