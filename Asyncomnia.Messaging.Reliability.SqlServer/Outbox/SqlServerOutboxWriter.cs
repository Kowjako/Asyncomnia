using System.Data;
using System.Data.Common;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Infrastructure.Connection;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Outbox;

internal sealed class SqlServerOutboxWriter(IDbTransactionAccessor transactionAccessor, ISqlServerConnectionFactory connectionFactory) : IOutboxWriter
{
    private const string InsertSql = $@"
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

    private readonly IDbTransactionAccessor _transactionAccessor = transactionAccessor
        ?? throw new ArgumentNullException(nameof(transactionAccessor));

    private readonly ISqlServerConnectionFactory _connectionFactory = connectionFactory
        ?? throw new ArgumentNullException(nameof(connectionFactory));

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
        await using var connection = await _connectionFactory.OpenConnectionAsync(token);

        await ExecuteInsertAsync(connection, transaction: null, message, token);
    }

    private static async Task WriteUsingTransactionAsync(OutboxMessage message, IDbTransaction transaction, CancellationToken token)
    {
        var connection = transaction.Connection
            ?? throw new InvalidOperationException("The current Outbox transaction is no longer associated with a database connection.");

        if (connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException("The database connection associated with the current Outbox transaction must be open.");
        }

        await ExecuteInsertAsync(connection, transaction, message, token);
    }

    private static async Task ExecuteInsertAsync(IDbConnection connection, IDbTransaction? transaction, OutboxMessage message, CancellationToken token)
    {
        using var command = connection.CreateCommand();
        command.CommandText = InsertSql;
        command.Transaction = transaction;

        AddParameter(command, "@MessageId", DbType.Guid, message.MessageId);
        AddParameter(command, "@ContractType", DbType.String, message.ContractType, size: 1000);
        AddParameter(command, "@MessageJson", DbType.String, message.MessageJson, size: -1);
        AddParameter(command, "@CorrelationId", DbType.Guid, message.CorrelationId);
        AddParameter(command, "@CreatedAt", DbType.DateTimeOffset, message.CreatedAt);
        AddParameter(command, "@AvailableAt", DbType.DateTimeOffset, message.CreatedAt);

        if (command is not DbCommand dbCommand)
        {
            throw new InvalidOperationException("The database command must support asynchronous execution.");
        }

        await dbCommand.ExecuteNonQueryAsync(token);
    }

    private static void AddParameter(IDbCommand command, string name, DbType type, object? value, int? size = null)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = type;
        parameter.Value = value ?? DBNull.Value;

        if (size.HasValue)
        {
            parameter.Size = size.Value;
        }

        command.Parameters.Add(parameter);
    }
}
