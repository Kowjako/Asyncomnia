using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Outbox.Processing.Retention;

internal sealed class OutboxCleanupProcessor(IOutboxCleanupStore store, IOptions<OutboxOptions> options)
{
    private readonly OutboxOptions _options = options.Value;

    public Task<int> ProcessBatchAsync(CancellationToken token = default)
    {
        return store.DeleteExpiredAsync(_options.ProcessedMessageRetention, _options.FailedMessageRetention,
            _options.CleanupBatchSize, token);
    }
}