using Asyncomnia.Messaging.Reliability.DelayedPublishing.Configuration;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.DelayedPublishing.Processing;

internal sealed class DelayedMessageBatchProcessor(IDelayedMessageStore store, IOptions<DelayedPublishingOptions> options, TimeProvider timeProvider)
{
    private readonly DelayedPublishingOptions _options = options.Value;

    public Task<int> ProcessBatchAsync(CancellationToken token = default)
    {
        var utcNow = timeProvider.GetUtcNow();
        return store.MoveDueToOutboxAsync(utcNow, _options.BatchSize, token);
    }
}
