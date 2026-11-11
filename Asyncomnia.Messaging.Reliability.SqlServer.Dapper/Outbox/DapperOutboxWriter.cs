using System.Data;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Transactions;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Outbox;

internal sealed class DapperOutboxWriter(
    IOutboxTransactionAccessor transactionAccessor,
    IOptions<SqlServerOutboxOptions> options) : IOutboxWriter
{
    private readonly IOutboxTransactionAccessor _transactionAccessor = transactionAccessor
        ?? throw new ArgumentNullException(nameof(transactionAccessor));

    private readonly string _connectionString = options?.Value.ConnectionString
        ?? throw new ArgumentNullException(nameof(options));

    public async Task WriteAsync(OutboxMessage message, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        token.ThrowIfCancellationRequested();

        if (_transactionAccessor.CurrentTransaction is { } transaction)
        {
            await WriteUsingTransactionAsync(message, transaction, token);
            return;
        }

        await WriteStandaloneAsync(message, token);
    }

    private async Task WriteStandaloneAsync(OutboxMessage message, CancellationToken token)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token);

        var command = CreateCommand(message, transaction: null, token);

        await connection.ExecuteAsync(command);
    }

    private static async Task WriteUsingTransactionAsync(
        OutboxMessage message,
        IDbTransaction transaction,
        CancellationToken token)
    {
        var connection = transaction.Connection
            ?? throw new InvalidOperationException("The current Outbox transaction is no longer associated with a database connection.");

        if (connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException("The database connection associated with the current Outbox transaction must be open.");
        }

        var command = CreateCommand(message, transaction, token);

        await connection.ExecuteAsync(command);
    }

    private static CommandDefinition CreateCommand(
        OutboxMessage message,
        IDbTransaction? transaction,
        CancellationToken token)
    {
        var sql = $@"
            INSERT INTO {SqlServerOutboxDefaults.QualifiedTableName}
            (
                [MessageId],
                [ContractType],
                [MessageJson],
                [CorrelationId],
                [CreatedAt],
                [AvailableAt]
            )
            VALUES
            (
                @MessageId,
                @ContractType,
                @MessageJson,
                @CorrelationId,
                @CreatedAt,
                @AvailableAt
            )";

        return new CommandDefinition(sql, new
        {
            message.MessageId,
            message.ContractType,
            message.MessageJson,
            message.CorrelationId,
            message.CreatedAt,
            AvailableAt = message.CreatedAt
        }, transaction: transaction, cancellationToken: token);
    }
}
