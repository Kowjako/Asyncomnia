namespace Asyncomnia.Messaging.Reliability.Inbox.Storage;

public interface IInboxCleanupStore
{
    Task<int> DeleteExpiredAsync(DateTimeOffset expiresBefore, int batchSize, CancellationToken token = default);
}
