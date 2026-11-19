using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Asyncomnia.Messaging.Reliability.Outbox.Processing.Retention;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Microsoft.Extensions.Options;
using Moq;

namespace Asyncomnia.Messaging.Reliability.Tests.Outbox.Processing;

public sealed class OutboxCleanupProcessorTests
{
    [Fact]
    public async Task ProcessBatchAsync_UsesConfiguredRetentionAndBatchSize()
    {
        var options = new OutboxOptions();
        options.ConfigureCleanup(
            TimeSpan.FromDays(4),
            TimeSpan.FromDays(21),
            options.CleanupInterval,
            73);
        using var cancellation = new CancellationTokenSource();
        var store = new Mock<IOutboxCleanupStore>(MockBehavior.Strict);
        store
            .Setup(x => x.DeleteExpiredAsync(
                options.ProcessedMessageRetention,
                options.FailedMessageRetention,
                options.CleanupBatchSize,
                cancellation.Token))
            .ReturnsAsync(19);
        var processor = new OutboxCleanupProcessor(
            store.Object,
            Options.Create(options));

        var deletedCount = await processor.ProcessBatchAsync(cancellation.Token);

        Assert.Equal(19, deletedCount);
        store.VerifyAll();
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenFailedRetentionIsNotConfigured_PassesNullToStore()
    {
        var options = new OutboxOptions();
        var store = new Mock<IOutboxCleanupStore>(MockBehavior.Strict);
        store
            .Setup(x => x.DeleteExpiredAsync(
                options.ProcessedMessageRetention,
                null,
                options.CleanupBatchSize,
                CancellationToken.None))
            .ReturnsAsync(0);
        var processor = new OutboxCleanupProcessor(
            store.Object,
            Options.Create(options));

        var deletedCount = await processor.ProcessBatchAsync();

        Assert.Equal(0, deletedCount);
        store.VerifyAll();
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenStoreFails_PropagatesException()
    {
        var expectedException = new InvalidOperationException("Database unavailable");
        var options = new OutboxOptions();
        var store = new Mock<IOutboxCleanupStore>(MockBehavior.Strict);
        store
            .Setup(x => x.DeleteExpiredAsync(
                options.ProcessedMessageRetention,
                options.FailedMessageRetention,
                options.CleanupBatchSize,
                CancellationToken.None))
            .ThrowsAsync(expectedException);
        var processor = new OutboxCleanupProcessor(
            store.Object,
            Options.Create(options));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            processor.ProcessBatchAsync());

        Assert.Same(expectedException, exception);
    }
}
