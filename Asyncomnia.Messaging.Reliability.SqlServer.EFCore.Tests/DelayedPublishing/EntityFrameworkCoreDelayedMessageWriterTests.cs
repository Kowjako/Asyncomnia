using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.TestDoubles;
using Microsoft.EntityFrameworkCore;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.DelayedPublishing;

public sealed class EntityFrameworkCoreDelayedMessageWriterTests
{
    [Fact]
    public async Task WriteAsync_WithMessage_AddsEntryWithoutSavingChanges()
    {
        await using var context = CreateContext();
        var writer = new EntityFrameworkCoreDelayedMessageWriter<TestDbContext>(context);
        var message = CreateMessage();

        await writer.WriteAsync(message);

        var trackedEntry = Assert.Single(
            context.ChangeTracker.Entries<DelayedMessageEntry>());
        Assert.Equal(EntityState.Added, trackedEntry.State);
        Assert.Equal(message.ScheduleId, trackedEntry.Entity.ScheduleId);
        Assert.Equal(message.MessageId, trackedEntry.Entity.MessageId);
        Assert.Equal(message.ContractType, trackedEntry.Entity.ContractType);
        Assert.Equal(message.MessageJson, trackedEntry.Entity.MessageJson);
        Assert.Equal(message.CorrelationId, trackedEntry.Entity.CorrelationId);
        Assert.Equal(message.CreatedAt, trackedEntry.Entity.CreatedAt);
        Assert.Equal(message.DeliverAt, trackedEntry.Entity.DeliverAt);
        Assert.Equal(0, context.SaveChangesAsyncCallCount);
    }

    [Fact]
    public async Task WriteAsync_WhenMessageIsNull_ThrowsArgumentNullException()
    {
        await using var context = CreateContext();
        var writer = new EntityFrameworkCoreDelayedMessageWriter<TestDbContext>(context);

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            writer.WriteAsync(null!));
    }

    [Fact]
    public async Task WriteAsync_WhenCancellationIsRequested_DoesNotAddEntry()
    {
        await using var context = CreateContext();
        var writer = new EntityFrameworkCoreDelayedMessageWriter<TestDbContext>(context);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            writer.WriteAsync(CreateMessage(), cancellation.Token));

        Assert.Empty(context.ChangeTracker.Entries<DelayedMessageEntry>());
    }

    [Fact]
    public async Task CancelAsync_WhenEntryIsPending_DetachesEntryWithoutSavingChanges()
    {
        await using var context = CreateContext();
        var writer = new EntityFrameworkCoreDelayedMessageWriter<TestDbContext>(context);
        var message = CreateMessage();
        await writer.WriteAsync(message);

        var cancelled = await writer.CancelAsync(message.ScheduleId);

        Assert.True(cancelled);
        Assert.Empty(context.ChangeTracker.Entries<DelayedMessageEntry>());
        Assert.Equal(0, context.SaveChangesAsyncCallCount);
    }

    [Fact]
    public async Task CancelAsync_WhenScheduleIdIsEmpty_ThrowsArgumentException()
    {
        await using var context = CreateContext();
        var writer = new EntityFrameworkCoreDelayedMessageWriter<TestDbContext>(context);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            writer.CancelAsync(Guid.Empty));
    }

    [Fact]
    public async Task CancelAsync_WhenCancellationIsRequested_ThrowsOperationCanceledException()
    {
        await using var context = CreateContext();
        var writer = new EntityFrameworkCoreDelayedMessageWriter<TestDbContext>(context);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            writer.CancelAsync(Guid.NewGuid(), cancellation.Token));
    }

    [Fact]
    public void Constructor_WhenDelayedPublishingIsNotConfigured_ThrowsInvalidOperationException()
    {
        var options = new DbContextOptionsBuilder<UnconfiguredDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .Options;
        using var context = new UnconfiguredDbContext(options);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            new EntityFrameworkCoreDelayedMessageWriter<UnconfiguredDbContext>(context));

        Assert.Contains(
            "modelBuilder.AddAsyncomniaDelayedPublishing()",
            exception.Message);
    }

    private static TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .Options;

        return new TestDbContext(options);
    }

    private static DelayedMessage CreateMessage()
    {
        var createdAt = new DateTimeOffset(2026, 11, 15, 10, 0, 0, TimeSpan.Zero);

        return new DelayedMessage(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "Tests.ReportRequested, Tests",
            "{\"reportId\":42}",
            Guid.NewGuid(),
            createdAt,
            createdAt.AddHours(1));
    }
}
