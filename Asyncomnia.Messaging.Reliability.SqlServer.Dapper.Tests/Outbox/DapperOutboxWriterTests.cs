using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Tests.Outbox;

public sealed class DapperOutboxWriterTests
{
    [Fact]
    public void Constructor_WhenTransactionAccessorIsNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new DapperOutboxWriter(null!));
    }

    [Fact]
    public async Task WriteAsync_WhenMessageIsNull_ThrowsArgumentNullException()
    {
        var writer = new DapperOutboxWriter(new TestUnitOfWork());

        await Assert.ThrowsAsync<ArgumentNullException>(() => writer.WriteAsync(null!));
    }

    [Fact]
    public async Task WriteAsync_WithoutActiveTransaction_ThrowsInvalidOperationException()
    {
        var writer = new DapperOutboxWriter(new TestUnitOfWork());

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.WriteAsync(CreateMessage()));

        Assert.Contains("active database transaction", exception.Message);
    }

    [Fact]
    public async Task WriteAsync_WhenCancellationIsRequested_ThrowsOperationCanceledException()
    {
        var writer = new DapperOutboxWriter(new TestUnitOfWork());
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            writer.WriteAsync(CreateMessage(), cancellation.Token));
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
