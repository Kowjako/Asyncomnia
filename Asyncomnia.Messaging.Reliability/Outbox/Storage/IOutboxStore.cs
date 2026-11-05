namespace Asyncomnia.Messaging.Reliability.Outbox.Storage;

public interface IOutboxStore
{
    Task<IReadOnlyList<ClaimedOutboxMessage>> ClaimAsync(int batchSize, TimeSpan leaseDuration, CancellationToken token = default);
    Task<bool> CompleteAsync(Guid messageId, Guid leaseId, CancellationToken token = default);
    Task<bool> ScheduleRetryAsync(Guid messageId, Guid leaseId, string error, TimeSpan retryDelay, CancellationToken token = default);
    Task<bool> FailAsync(Guid messageId, Guid leaseId, string error, CancellationToken token = default);
}