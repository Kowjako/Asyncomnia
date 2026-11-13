using System.Data;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Tests.TestDoubles;
using Microsoft.Data.SqlClient;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.Inbox;

public sealed class SqlServerInboxWriterTests
{
    [Fact]
    public void Constructor_WhenTransactionIsNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new SqlServerInboxWriter(null!));
    }

    [Fact]
    public async Task TryAddAsync_WhenMessageIsNull_ThrowsArgumentNullException()
    {
        var writer = new SqlServerInboxWriter(new TestUnitOfWork());

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            writer.TryAddAsync(null!));
    }

    [Fact]
    public async Task TryAddAsync_WhenCancellationIsRequested_ThrowsOperationCanceledException()
    {
        var writer = new SqlServerInboxWriter(new TestUnitOfWork());
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            writer.TryAddAsync(CreateMessage(), cancellation.Token));
    }

    [Fact]
    public async Task TryAddAsync_WithoutActiveTransaction_ThrowsInvalidOperationException()
    {
        var writer = new SqlServerInboxWriter(new TestUnitOfWork());

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.TryAddAsync(CreateMessage()));

        Assert.Contains("active inbox transaction", exception.Message);
    }

    [Fact]
    public async Task TryAddAsync_WhenCurrentTransactionHasNoConnection_ThrowsInvalidOperationException()
    {
        var unitOfWork = new TestUnitOfWork
        {
            CurrentTransaction = new TestDbTransaction(null)
        };
        var writer = new SqlServerInboxWriter(unitOfWork);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.TryAddAsync(CreateMessage()));

        Assert.Contains("no longer associated", exception.Message);
    }

    [Fact]
    public async Task TryAddAsync_WhenCurrentTransactionConnectionIsClosed_ThrowsInvalidOperationException()
    {
        await using var connection = new SqlConnection();
        var unitOfWork = new TestUnitOfWork
        {
            CurrentTransaction = new TestDbTransaction(connection)
        };
        var writer = new SqlServerInboxWriter(unitOfWork);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.TryAddAsync(CreateMessage()));

        Assert.Contains("must be open", exception.Message);
    }

    private static InboxMessage CreateMessage()
    {
        return new InboxMessage(
            Guid.NewGuid(),
            "events/clients/subscription/billing-service",
            "Tests.ClientCreatedEvent, Tests",
            new DateTimeOffset(2026, 11, 12, 10, 30, 0, TimeSpan.Zero));
    }

    private sealed class TestDbTransaction(IDbConnection? connection) : IDbTransaction
    {
        public IDbConnection? Connection { get; } = connection;

        public IsolationLevel IsolationLevel => IsolationLevel.Unspecified;

        public void Commit()
        {
            throw new NotSupportedException();
        }

        public void Rollback()
        {
            throw new NotSupportedException();
        }

        public void Dispose()
        { }
    }
}
