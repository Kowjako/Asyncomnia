using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.TestDoubles;
using Microsoft.EntityFrameworkCore;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.Outbox;

public sealed class EntityFrameworkCoreOutboxWriterTests
{
    [Fact]
    public async Task WriteAsync_WithMessage_AddsEntryWithoutSavingChanges()
    {
        await using var context = CreateContext();
        var writer = new EntityFrameworkCoreOutboxWriter<TestDbContext>(context);
        var message = CreateMessage();

        await writer.WriteAsync(message);

        var trackedEntry = Assert.Single(context.ChangeTracker.Entries<OutboxEntry>());
        Assert.Equal(EntityState.Added, trackedEntry.State);
        Assert.Equal(message.MessageId, trackedEntry.Entity.MessageId);
        Assert.Equal(message.ContractType, trackedEntry.Entity.ContractType);
        Assert.Equal(message.MessageJson, trackedEntry.Entity.MessageJson);
        Assert.Equal(message.CorrelationId, trackedEntry.Entity.CorrelationId);
        Assert.Equal(message.CreatedAt, trackedEntry.Entity.CreatedAt);
        Assert.Equal(message.CreatedAt, trackedEntry.Entity.AvailableAt);
        Assert.Equal(0, context.SaveChangesAsyncCallCount);
    }

    [Fact]
    public async Task WriteAsync_WhenCancellationIsRequested_DoesNotAddEntry()
    {
        await using var context = CreateContext();
        var writer = new EntityFrameworkCoreOutboxWriter<TestDbContext>(context);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            writer.WriteAsync(CreateMessage(), cancellation.Token));

        Assert.Empty(context.ChangeTracker.Entries<OutboxEntry>());
    }

    [Fact]
    public void Constructor_WhenOutboxIsNotConfigured_ThrowsInvalidOperationException()
    {
        var options = new DbContextOptionsBuilder<UnconfiguredDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .Options;
        using var context = new UnconfiguredDbContext(options);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            new EntityFrameworkCoreOutboxWriter<UnconfiguredDbContext>(context));

        Assert.Contains("modelBuilder.AddAsyncomniaOutbox()", exception.Message);
    }

    private static TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .Options;

        return new TestDbContext(options);
    }

    private static OutboxMessage CreateMessage()
    {
        return new OutboxMessage(
            Guid.NewGuid(),
            "Tests.OrderCreated, Tests",
            "{\"orderId\":42}",
            Guid.NewGuid(),
            new DateTimeOffset(2026, 11, 6, 10, 30, 0, TimeSpan.Zero));
    }
}
