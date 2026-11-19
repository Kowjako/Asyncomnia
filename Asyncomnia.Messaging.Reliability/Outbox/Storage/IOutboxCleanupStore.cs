namespace Asyncomnia.Messaging.Reliability.Outbox.Storage;

public interface IOutboxCleanupStore
{
    Task<int> DeleteExpiredAsync(TimeSpan processedMessageRetention, TimeSpan? failedMessageRetention, int batchSize, CancellationToken token = default);
}
