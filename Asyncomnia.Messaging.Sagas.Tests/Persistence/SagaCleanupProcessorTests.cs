using Asyncomnia.Messaging.Sagas.Configuration;
using Asyncomnia.Messaging.Sagas.Persistence.Retention;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Sagas.Tests.Persistence;

public sealed class SagaCleanupProcessorTests
{
    [Fact]
    public async Task ProcessBatchAsync_UsesConfiguredRetentionAndBatchSize()
    {
        var options = new SagaOptions
        {
            FinalizedSagaRetention = TimeSpan.FromDays(17),
            CleanupBatchSize = 41
        };
        using var cancellation = new CancellationTokenSource();
        var store = new RecordingSagaCleanupStore
        {
            DeletedCount = 13
        };
        var processor = new SagaCleanupProcessor(
            store,
            Options.Create(options));

        var deletedCount = await processor.ProcessBatchAsync(cancellation.Token);

        Assert.Equal(13, deletedCount);
        var call = Assert.Single(store.Calls);
        Assert.Equal(options.FinalizedSagaRetention, call.Retention);
        Assert.Equal(options.CleanupBatchSize, call.BatchSize);
        Assert.Equal(cancellation.Token, call.Token);
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenStoreFails_PropagatesException()
    {
        var expectedException = new InvalidOperationException("Database unavailable");
        var store = new RecordingSagaCleanupStore
        {
            Exception = expectedException
        };
        var processor = new SagaCleanupProcessor(
            store,
            Options.Create(new SagaOptions()));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            processor.ProcessBatchAsync());

        Assert.Same(expectedException, exception);
    }

    private sealed class RecordingSagaCleanupStore : ISagaCleanupStore
    {
        public List<(TimeSpan Retention, int BatchSize, CancellationToken Token)> Calls { get; } = [];
        public int DeletedCount { get; init; }
        public Exception? Exception { get; init; }

        public Task<int> DeleteFinalizedAsync(
            TimeSpan finalizedSagaRetention,
            int batchSize,
            CancellationToken token = default)
        {
            Calls.Add((finalizedSagaRetention, batchSize, token));

            return Exception is null
                ? Task.FromResult(DeletedCount)
                : Task.FromException<int>(Exception);
        }
    }
}
