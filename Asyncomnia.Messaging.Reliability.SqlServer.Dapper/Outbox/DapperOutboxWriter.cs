using System.Data;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Transactions;
using Dapper;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Outbox;

internal sealed class DapperOutboxWriter(IOutboxTransactionAccessor transactionAccessor) : IOutboxWriter
{
    private readonly IOutboxTransactionAccessor _transactionAccessor = transactionAccessor
        ?? throw new ArgumentNullException(nameof(transactionAccessor));

    public async Task WriteAsync(OutboxMessage message, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        token.ThrowIfCancellationRequested();

        var transaction = _transactionAccessor.CurrentTransaction
            ?? throw new InvalidOperationException("An active database transaction is required to write an Outbox message using Dapper.");

        var connection = transaction.Connection
            ?? throw new InvalidOperationException("The current Outbox transaction is no longer associated with a database connection.");

        if (connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException("The database connection associated with the current Outbox transaction must be open.");
        }

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

        var command = new CommandDefinition(
            sql,
            new
            {
                message.MessageId,
                message.ContractType,
                message.MessageJson,
                message.CorrelationId,
                message.CreatedAt,
                AvailableAt = message.CreatedAt
            },
            transaction: transaction,
            cancellationToken: token);

        await connection.ExecuteAsync(command);
    }
}