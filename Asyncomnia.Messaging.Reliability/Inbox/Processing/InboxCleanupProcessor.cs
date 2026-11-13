using Asyncomnia.Messaging.Reliability.Inbox.Configuration;
using Asyncomnia.Messaging.Reliability.Inbox.Storage;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Inbox.Processing;

internal sealed class InboxCleanupProcessor(IInboxCleanupStore store, IOptions<InboxOptions> options, TimeProvider timeProvider)
{
    private readonly InboxOptions _options = options.Value;

    public Task<int> ProcessBatchAsync(CancellationToken token = default)
    {
        var expiresBefore = timeProvider.GetUtcNow() - _options.DuplicateDetectionWindow;

        return store.DeleteExpiredAsync(expiresBefore, _options.CleanupBatchSize, token);
    }
}