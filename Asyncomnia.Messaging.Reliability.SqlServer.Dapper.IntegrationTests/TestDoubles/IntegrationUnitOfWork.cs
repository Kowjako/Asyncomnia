using System.Data;
using System.Data.Common;
using Dapper;
using Microsoft.Data.SqlClient;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.IntegrationTests.TestDoubles;

internal sealed class IntegrationUnitOfWork : IIntegrationUnitOfWork, IAsyncDisposable
{
    private readonly SqlConnection _connection;
    private DbTransaction? _transaction;

    public DbTransaction? CurrentTransaction => _transaction;

    public IntegrationUnitOfWork(string connectionString)
    {
        _connection = new SqlConnection(connectionString);
    }

    public async Task BeginAsync(CancellationToken token = default)
    {
        if (_transaction is not null)
        {
            throw new InvalidOperationException("A transaction is already active.");
        }

        if (_connection.State != ConnectionState.Open)
        {
            await _connection.OpenAsync(token);
        }

        _transaction = await _connection.BeginTransactionAsync(token);
    }

    public async Task InsertOrderAsync(Guid id, string description, CancellationToken token = default)
    {
        var transaction = GetRequiredTransaction();
        const string sql = "INSERT INTO [dbo].[Orders] ([Id], [Description]) VALUES (@Id, @Description)";
        var command = new CommandDefinition(
            sql,
            new { Id = id, Description = description },
            transaction: transaction,
            cancellationToken: token);

        await _connection.ExecuteAsync(command);
    }

    public async Task CommitAsync(CancellationToken token = default)
    {
        var transaction = GetRequiredTransaction();

        await transaction.CommitAsync(token);
        await transaction.DisposeAsync();
        _transaction = null;
    }

    public async Task RollbackAsync(CancellationToken token = default)
    {
        var transaction = GetRequiredTransaction();

        await transaction.RollbackAsync(token);
        await transaction.DisposeAsync();
        _transaction = null;
    }

    public async ValueTask DisposeAsync()
    {
        if (_transaction is not null)
        {
            await _transaction.DisposeAsync();
            _transaction = null;
        }

        await _connection.DisposeAsync();
    }

    private DbTransaction GetRequiredTransaction()
    {
        return _transaction
            ?? throw new InvalidOperationException("An active transaction is required.");
    }
}
