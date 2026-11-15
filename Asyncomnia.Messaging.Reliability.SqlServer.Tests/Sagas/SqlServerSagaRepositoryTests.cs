using System.Data;
using Asyncomnia.Messaging.Reliability.SqlServer.Sagas;
using Asyncomnia.Messaging.Reliability.SqlServer.Tests.TestDoubles;
using Asyncomnia.Messaging.Sagas.StateMachine;
using Microsoft.Data.SqlClient;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.Sagas;

public sealed class SqlServerSagaRepositoryTests
{
    [Fact]
    public void Constructor_WhenTransactionAccessorIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new SqlServerSagaRepository<TestSagaInstance>(null!));

        Assert.Equal("dbTransactionAccessor", exception.ParamName);
    }

    [Fact]
    public async Task GetForUpdateAsync_WhenCorrelationIdIsEmpty_ThrowsArgumentException()
    {
        var repository = CreateRepository();

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            repository.GetForUpdateAsync(Guid.Empty, default));

        Assert.Equal("correlationId", exception.ParamName);
    }

    [Fact]
    public async Task GetForUpdateAsync_WhenCancellationIsRequested_ThrowsOperationCanceledException()
    {
        var repository = CreateRepository();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            repository.GetForUpdateAsync(
                Guid.NewGuid(),
                cancellationTokenSource.Token));
    }

    [Fact]
    public async Task GetForUpdateAsync_WithoutActiveTransaction_ThrowsInvalidOperationException()
    {
        var repository = CreateRepository();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            repository.GetForUpdateAsync(Guid.NewGuid(), default));

        Assert.Contains("active saga transaction", exception.Message);
    }

    [Fact]
    public async Task GetForUpdateAsync_WhenTransactionHasNoConnection_ThrowsInvalidOperationException()
    {
        var unitOfWork = new TestUnitOfWork
        {
            CurrentTransaction = new TestDbTransaction(null)
        };
        var repository = new SqlServerSagaRepository<TestSagaInstance>(unitOfWork);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            repository.GetForUpdateAsync(Guid.NewGuid(), default));

        Assert.Contains("no longer associated", exception.Message);
    }

    [Fact]
    public async Task GetForUpdateAsync_WhenTransactionConnectionIsClosed_ThrowsInvalidOperationException()
    {
        await using var connection = new SqlConnection();
        var unitOfWork = new TestUnitOfWork
        {
            CurrentTransaction = new TestDbTransaction(connection)
        };
        var repository = new SqlServerSagaRepository<TestSagaInstance>(unitOfWork);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            repository.GetForUpdateAsync(Guid.NewGuid(), default));

        Assert.Contains("must be open", exception.Message);
    }

    [Fact]
    public async Task TryInsertAsync_WhenInstanceIsNull_ThrowsArgumentNullException()
    {
        var repository = CreateRepository();

        var exception = await Assert.ThrowsAsync<ArgumentNullException>(() =>
            repository.TryInsertAsync(null!, default));

        Assert.Equal("instance", exception.ParamName);
    }

    [Fact]
    public async Task TryInsertAsync_WhenCorrelationIdIsEmpty_ThrowsArgumentException()
    {
        var repository = CreateRepository();
        var instance = new TestSagaInstance();

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            repository.TryInsertAsync(instance, default));

        Assert.Equal("instance", exception.ParamName);
        Assert.Contains("correlation ID", exception.Message);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public async Task TryInsertAsync_WhenCurrentStateIsInvalid_ThrowsArgumentException(
        string? currentState)
    {
        var repository = CreateRepository();
        var instance = CreateInstance();
        instance.CurrentState = currentState!;

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            repository.TryInsertAsync(instance, default));

        Assert.Equal("instance", exception.ParamName);
        Assert.Contains("current state", exception.Message);
    }

    [Fact]
    public async Task TryInsertAsync_WhenCancellationIsRequested_ThrowsOperationCanceledException()
    {
        var repository = CreateRepository();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            repository.TryInsertAsync(
                CreateInstance(),
                cancellationTokenSource.Token));
    }

    [Fact]
    public async Task TryInsertAsync_WithoutActiveTransaction_ThrowsInvalidOperationException()
    {
        var repository = CreateRepository();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            repository.TryInsertAsync(CreateInstance(), default));

        Assert.Contains("active saga transaction", exception.Message);
    }

    [Fact]
    public async Task UpdateAsync_WhenInstanceIsNull_ThrowsArgumentNullException()
    {
        var repository = CreateRepository();

        var exception = await Assert.ThrowsAsync<ArgumentNullException>(() =>
            repository.UpdateAsync(null!, default));

        Assert.Equal("instance", exception.ParamName);
    }

    [Fact]
    public async Task UpdateAsync_WhenCancellationIsRequested_ThrowsOperationCanceledException()
    {
        var repository = CreateRepository();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            repository.UpdateAsync(
                CreateInstance(),
                cancellationTokenSource.Token));
    }

    [Fact]
    public async Task UpdateAsync_WithoutActiveTransaction_ThrowsInvalidOperationException()
    {
        var repository = CreateRepository();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            repository.UpdateAsync(CreateInstance(), default));

        Assert.Contains("active saga transaction", exception.Message);
    }

    private static SqlServerSagaRepository<TestSagaInstance> CreateRepository()
    {
        return new SqlServerSagaRepository<TestSagaInstance>(new TestUnitOfWork());
    }

    private static TestSagaInstance CreateInstance()
    {
        return new TestSagaInstance
        {
            CorrelationId = Guid.NewGuid(),
            CurrentState = "Submitted",
            Value = "test-value"
        };
    }

    public sealed class TestSagaInstance : AsyncomniaSagaInstance
    {
        public string Value { get; set; } = null!;
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
