using System.Data;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Tests.TestDoubles;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Tests.Outbox;

public sealed class DapperOutboxWriterTests
{
    [Fact]
    public void Constructor_WhenTransactionAccessorIsNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new DapperOutboxWriter(null!, CreateOptions()));
    }

    [Fact]
    public void Constructor_WhenOptionsAreNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new DapperOutboxWriter(new TestUnitOfWork(), null!));
    }

    [Fact]
    public async Task WriteAsync_WhenMessageIsNull_ThrowsArgumentNullException()
    {
        var writer = new DapperOutboxWriter(new TestUnitOfWork(), CreateOptions());

        await Assert.ThrowsAsync<ArgumentNullException>(() => writer.WriteAsync(null!));
    }

    [Fact]
    public async Task WriteAsync_WhenCurrentTransactionHasNoConnection_ThrowsInvalidOperationException()
    {
        var unitOfWork = new TestUnitOfWork
        {
            CurrentTransaction = new TestDbTransaction(null)
        };
        var writer = new DapperOutboxWriter(unitOfWork, CreateOptions());

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.WriteAsync(CreateMessage()));

        Assert.Contains("no longer associated", exception.Message);
    }

    [Fact]
    public async Task WriteAsync_WhenCurrentTransactionConnectionIsClosed_ThrowsInvalidOperationException()
    {
        await using var connection = new SqlConnection();
        var unitOfWork = new TestUnitOfWork
        {
            CurrentTransaction = new TestDbTransaction(connection)
        };
        var writer = new DapperOutboxWriter(unitOfWork, CreateOptions());

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.WriteAsync(CreateMessage()));

        Assert.Contains("must be open", exception.Message);
    }

    [Fact]
    public async Task WriteAsync_WhenCancellationIsRequested_ThrowsOperationCanceledException()
    {
        var writer = new DapperOutboxWriter(new TestUnitOfWork(), CreateOptions());
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

    private static IOptions<SqlServerOutboxOptions> CreateOptions()
    {
        return Options.Create(new SqlServerOutboxOptions
        {
            ConnectionString = "Server=localhost;Database=AsyncomniaTests"
        });
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
        {
        }
    }
}
