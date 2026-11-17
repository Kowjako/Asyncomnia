using Asyncomnia.Messaging.Sagas.Configuration;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Sagas.Persistence.Retention;

internal sealed class SagaCleanupProcessor(ISagaCleanupStore store, IOptions<SagaOptions> options)
{
    private readonly SagaOptions _options = options.Value;

    public Task<int> ProcessBatchAsync(CancellationToken token = default)
    {
        return store.DeleteFinalizedAsync(_options.FinalizedSagaRetention, _options.CleanupBatchSize, token);
    }
}
