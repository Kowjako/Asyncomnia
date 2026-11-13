using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.DelayedPublishing.Configuration;
using Asyncomnia.Messaging.Reliability.DelayedPublishing.Processing;
using Asyncomnia.Messaging.Reliability.Tests.TestDoubles;
using Microsoft.Extensions.Options;
using Moq;

namespace Asyncomnia.Messaging.Reliability.Tests.DelayedPublishing;

public sealed class DelayedMessageBatchProcessorTests
{
    [Fact]
    public async Task ProcessBatchAsync_MovesDueMessagesUsingCurrentTimeAndBatchSize()
    {
        var utcNow = new DateTimeOffset(2026, 11, 15, 10, 0, 0, TimeSpan.Zero);
        var options = new DelayedPublishingOptions { BatchSize = 43 };
        using var cancellation = new CancellationTokenSource();
        var store = new Mock<IDelayedMessageStore>(MockBehavior.Strict);
        store
            .Setup(x => x.MoveDueToOutboxAsync(
                utcNow,
                options.BatchSize,
                cancellation.Token))
            .ReturnsAsync(17);
        var processor = new DelayedMessageBatchProcessor(
            store.Object,
            Options.Create(options),
            new FixedTimeProvider(utcNow));

        var movedCount = await processor.ProcessBatchAsync(cancellation.Token);

        Assert.Equal(17, movedCount);
        store.VerifyAll();
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenStoreFails_PropagatesException()
    {
        var expectedException = new InvalidOperationException("Database unavailable");
        var options = new DelayedPublishingOptions();
        var store = new Mock<IDelayedMessageStore>(MockBehavior.Strict);
        store
            .Setup(x => x.MoveDueToOutboxAsync(
                It.IsAny<DateTimeOffset>(),
                options.BatchSize,
                CancellationToken.None))
            .ThrowsAsync(expectedException);
        var processor = new DelayedMessageBatchProcessor(
            store.Object,
            Options.Create(options),
            new FixedTimeProvider(DateTimeOffset.UtcNow));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            processor.ProcessBatchAsync());

        Assert.Same(expectedException, exception);
    }
}
