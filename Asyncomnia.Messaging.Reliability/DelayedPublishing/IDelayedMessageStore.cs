namespace Asyncomnia.Messaging.Reliability.DelayedPublishing;

public interface IDelayedMessageStore
{
    Task<int> MoveDueToOutboxAsync(DateTimeOffset utcNow, int batchSize, CancellationToken token = default);
}
