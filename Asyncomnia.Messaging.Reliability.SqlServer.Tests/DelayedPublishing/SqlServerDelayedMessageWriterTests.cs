using System.Data;
using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.Tests.TestDoubles;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.DelayedPublishing;

public sealed class SqlServerDelayedMessageWriterTests
{
    [Fact]
    public void Constructor_WhenTransactionAccessorIsNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new SqlServerDelayedMessageWriter(null!, CreateOptions()));
    }

    [Fact]
    public void Constructor_WhenOptionsAreNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new SqlServerDelayedMessageWriter(new TestUnitOfWork(), null!));
    }

    [Fact]
    public async Task WriteAsync_WhenMessageIsNull_ThrowsArgumentNullException()
    {
        var writer = CreateWriter(new TestUnitOfWork());

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            writer.WriteAsync(null!));
    }

    [Fact]
    public async Task WriteAsync_WhenCancellationIsRequested_ThrowsOperationCanceledException()
    {
        var writer = CreateWriter(new TestUnitOfWork());
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            writer.WriteAsync(CreateMessage(), cancellation.Token));
    }

    [Fact]
    public async Task WriteAsync_WhenCurrentTransactionHasNoConnection_ThrowsInvalidOperationException()
    {
        var unitOfWork = new TestUnitOfWork
        {
            CurrentTransaction = new TestDbTransaction(null)
        };
        var writer = CreateWriter(unitOfWork);

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
        var writer = CreateWriter(unitOfWork);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.WriteAsync(CreateMessage()));

        Assert.Contains("must be open", exception.Message);
    }

    [Fact]
    public async Task CancelAsync_WhenScheduleIdIsEmpty_ThrowsArgumentException()
    {
        var writer = CreateWriter(new TestUnitOfWork());

        await Assert.ThrowsAsync<ArgumentException>(() =>
            writer.CancelAsync(Guid.Empty));
    }

    [Fact]
    public async Task CancelAsync_WhenCancellationIsRequested_ThrowsOperationCanceledException()
    {
        var writer = CreateWriter(new TestUnitOfWork());
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            writer.CancelAsync(Guid.NewGuid(), cancellation.Token));
    }

    [Fact]
    public async Task CancelAsync_WhenCurrentTransactionHasNoConnection_ThrowsInvalidOperationException()
    {
        var unitOfWork = new TestUnitOfWork
        {
            CurrentTransaction = new TestDbTransaction(null)
        };
        var writer = CreateWriter(unitOfWork);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.CancelAsync(Guid.NewGuid()));

        Assert.Contains("no longer associated", exception.Message);
    }

    [Fact]
    public async Task CancelAsync_WhenCurrentTransactionConnectionIsClosed_ThrowsInvalidOperationException()
    {
        await using var connection = new SqlConnection();
        var unitOfWork = new TestUnitOfWork
        {
            CurrentTransaction = new TestDbTransaction(connection)
        };
        var writer = CreateWriter(unitOfWork);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.CancelAsync(Guid.NewGuid()));

        Assert.Contains("must be open", exception.Message);
    }

    private static SqlServerDelayedMessageWriter CreateWriter(TestUnitOfWork unitOfWork)
    {
        return new SqlServerDelayedMessageWriter(unitOfWork, CreateOptions());
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
            createdAt.AddMinutes(30));
    }

    private static IOptions<SqlServerOptions> CreateOptions()
    {
        return Options.Create(new SqlServerOptions
        {
            ConnectionString = "Server=localhost;Database=AsyncomniaTests"
        });
    }

    private sealed class TestDbTransaction(IDbConnection? connection) : IDbTransaction
    {
        public IDbConnection? Connection { get; } = connection;

        public IsolationLevel IsolationLevel => IsolationLevel.Unspecified;

        public void Commit() => throw new NotSupportedException();

        public void Rollback() => throw new NotSupportedException();

        public void Dispose()
        { }
    }
}
