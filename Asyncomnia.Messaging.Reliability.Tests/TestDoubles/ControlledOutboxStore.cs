using Asyncomnia.Messaging.Reliability.Outbox.Storage;

namespace Asyncomnia.Messaging.Reliability.Tests.TestDoubles;

internal sealed class ControlledOutboxStore(Func<int, TimeSpan, CancellationToken, Task<IReadOnlyList<ClaimedOutboxMessage>>> claimAsync) : IOutboxStore
{
    private int _claimCalls;

    public int ClaimCalls => Volatile.Read(ref _claimCalls);

    public Func<Guid, Guid, CancellationToken, Task<bool>> CompleteAsyncHandler { get; set; }
        = (_, _, _) => Task.FromResult(true);

    public Func<Guid, Guid, string, TimeSpan, CancellationToken, Task<bool>> ScheduleRetryAsyncHandler { get; set; }
        = (_, _, _, _, _) => Task.FromResult(true);

    public Func<Guid, Guid, string, CancellationToken, Task<bool>> FailAsyncHandler { get; set; }
        = (_, _, _, _) => Task.FromResult(true);

    public Task<IReadOnlyList<ClaimedOutboxMessage>> ClaimAsync(int batchSize, TimeSpan leaseDuration, CancellationToken token = default)
    {
        Interlocked.Increment(ref _claimCalls);
        return claimAsync(batchSize, leaseDuration, token);
    }

    public Task<bool> CompleteAsync(Guid messageId, Guid leaseId, CancellationToken token = default)
    {
        return CompleteAsyncHandler(messageId, leaseId, token);
    }

    public Task<bool> ScheduleRetryAsync(Guid messageId, Guid leaseId, string error, TimeSpan retryDelay, CancellationToken token = default)
    {
        return ScheduleRetryAsyncHandler(messageId, leaseId, error, retryDelay, token);
    }

    public Task<bool> FailAsync(Guid messageId, Guid leaseId, string error, CancellationToken token = default)
    {
        return FailAsyncHandler(messageId, leaseId, error, token);
    }
}
