using Asyncomnia.Messaging.Reliability.Inbox.Configuration;
using Asyncomnia.Messaging.Reliability.Inbox.Processing;
using Asyncomnia.Messaging.Reliability.Inbox.Storage;
using Asyncomnia.Messaging.Reliability.Tests.TestDoubles;
using Microsoft.Extensions.Options;
using Moq;

namespace Asyncomnia.Messaging.Reliability.Tests.Inbox;

public sealed class InboxCleanupProcessorTests
{
    [Fact]
    public async Task ProcessBatchAsync_DeletesMessagesOlderThanDetectionWindow()
    {
        var utcNow = new DateTimeOffset(2026, 11, 13, 12, 0, 0, TimeSpan.Zero);
        var options = new InboxOptions
        {
            DuplicateDetectionWindow = TimeSpan.FromHours(2),
            CleanupBatchSize = 37
        };
        using var cancellation = new CancellationTokenSource();
        var store = new Mock<IInboxCleanupStore>(MockBehavior.Strict);
        store
            .Setup(x => x.DeleteExpiredAsync(
                utcNow - options.DuplicateDetectionWindow,
                options.CleanupBatchSize,
                cancellation.Token))
            .ReturnsAsync(12);
        var processor = new InboxCleanupProcessor(
            store.Object,
            Options.Create(options),
            new FixedTimeProvider(utcNow));

        var deletedCount = await processor.ProcessBatchAsync(cancellation.Token);

        Assert.Equal(12, deletedCount);
        store.VerifyAll();
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenStoreFails_PropagatesException()
    {
        var expectedException = new InvalidOperationException("Database unavailable");
        var options = new InboxOptions();
        var store = new Mock<IInboxCleanupStore>(MockBehavior.Strict);
        store
            .Setup(x => x.DeleteExpiredAsync(
                It.IsAny<DateTimeOffset>(),
                options.CleanupBatchSize,
                CancellationToken.None))
            .ThrowsAsync(expectedException);
        var processor = new InboxCleanupProcessor(
            store.Object,
            Options.Create(options),
            new FixedTimeProvider(DateTimeOffset.UtcNow));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            processor.ProcessBatchAsync());

        Assert.Same(expectedException, exception);
    }
}
