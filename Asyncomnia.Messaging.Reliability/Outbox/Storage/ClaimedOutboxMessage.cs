namespace Asyncomnia.Messaging.Reliability.Outbox.Storage;

public sealed record ClaimedOutboxMessage
{
    public OutboxMessage Message { get; }
    public Guid LeaseId { get; }
    public int AttemptNumber { get; }
    public DateTimeOffset LeaseExpiresAt { get; }

    public ClaimedOutboxMessage(OutboxMessage message, Guid leaseId, int attemptNumber, DateTimeOffset leaseExpiresAt)
    {
        ArgumentNullException.ThrowIfNull(message, nameof(message));

        if (leaseId == Guid.Empty) 
            throw new ArgumentException("Lease ID cannot be empty", nameof(leaseId));

        if (attemptNumber <= 0)
            throw new ArgumentOutOfRangeException(nameof(attemptNumber), "Attempt number must be greater than zero");

        Message = message;
        LeaseId = leaseId;
        AttemptNumber = attemptNumber;
        LeaseExpiresAt = leaseExpiresAt;
    }
}
