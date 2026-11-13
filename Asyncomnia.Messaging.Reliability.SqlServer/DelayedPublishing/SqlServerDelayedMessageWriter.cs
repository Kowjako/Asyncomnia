using System.Data;
using System.Data.Common;
using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.DelayedPublishing;

internal sealed class SqlServerDelayedMessageWriter(IDbTransactionAccessor transactionAccessor, IOptions<SqlServerOptions> options)
    : IDelayedMessageWriter
{
    private const string InsertSql = $@"
        INSERT INTO {SqlServerDelayedDefaults.QualifiedTableName}
        (
            [ScheduleId],
            [MessageId],
            [ContractType],
            [MessageJson],
            [CorrelationId],
            [CreatedAt],
            [DeliverAt]
        )
        VALUES
        (
            @ScheduleId,
            @MessageId,
            @ContractType,
            @MessageJson,
            @CorrelationId,
            @CreatedAt,
            @DeliverAt
        )";

    private const string CancelSql = $@"
        DELETE FROM {SqlServerDelayedDefaults.QualifiedTableName}
        WHERE [ScheduleId] = @ScheduleId";

    private readonly IDbTransactionAccessor _transactionAccessor = transactionAccessor
        ?? throw new ArgumentNullException(nameof(transactionAccessor));

    private readonly string _connectionString = options?.Value.ConnectionString
        ?? throw new ArgumentNullException(nameof(options));

    public async Task<bool> CancelAsync(Guid scheduleId, CancellationToken token = default)
    {
        if (scheduleId == Guid.Empty)
        {
            throw new ArgumentException("Schedule ID cannot be empty", nameof(scheduleId));
        }

        token.ThrowIfCancellationRequested();

        if (_transactionAccessor.CurrentTransaction is { } transaction)
        {
            return await CancelUsingTransactionAsync(scheduleId, transaction, token);
        }

        return await CancelStandaloneAsync(scheduleId, token);
    }

    public async Task WriteAsync(DelayedMessage message, CancellationToken token = default)
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

    private async Task<bool> CancelStandaloneAsync(Guid scheduleId, CancellationToken token)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token);

        return await ExecuteCancelAsync(connection, transaction: null, scheduleId, token);
    }

    private async Task<bool> CancelUsingTransactionAsync(Guid scheduleId, IDbTransaction transaction, CancellationToken token)
    {
        var connection = transaction.Connection
            ?? throw new InvalidOperationException("The current Delayed message transaction is no longer associated with a database connection.");

        if (connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException("The database connection associated with the current Delayed message transaction must be open.");
        }

        return await ExecuteCancelAsync(connection, transaction, scheduleId, token);
    }

    private async Task WriteStandaloneAsync(DelayedMessage message, CancellationToken token)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token);

        await ExecuteInsertAsync(connection, transaction: null, message, token);
    }

    private async Task WriteUsingTransactionAsync(DelayedMessage message, IDbTransaction transaction, CancellationToken token)
    {
        var connection = transaction.Connection
            ?? throw new InvalidOperationException("The current Delayed message transaction is no longer associated with a database connection.");

        if (connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException("The database connection associated with the current Delayed message transaction must be open.");
        }

        await ExecuteInsertAsync(connection, transaction, message, token);
    }

    private static async Task ExecuteInsertAsync(IDbConnection connection, IDbTransaction? transaction, DelayedMessage message, CancellationToken token)
    {
        using var command = connection.CreateCommand();
        command.CommandText = InsertSql;
        command.Transaction = transaction;

        AddParameter(command, "@ScheduleId", DbType.Guid, message.ScheduleId);
        AddParameter(command, "@MessageId", DbType.Guid, message.MessageId);
        AddParameter(command, "@ContractType", DbType.String, message.ContractType, size: 1000);
        AddParameter(command, "@MessageJson", DbType.String, message.MessageJson, size: -1);
        AddParameter(command, "@CorrelationId", DbType.Guid, message.CorrelationId);
        AddParameter(command, "@CreatedAt", DbType.DateTimeOffset, message.CreatedAt);
        AddParameter(command, "@DeliverAt", DbType.DateTimeOffset, message.DeliverAt);

        if (command is not DbCommand dbCommand)
        {
            throw new InvalidOperationException("The database command must support asynchronous execution.");
        }

        await dbCommand.ExecuteNonQueryAsync(token);
    }

    private static async Task<bool> ExecuteCancelAsync(IDbConnection connection, IDbTransaction? transaction, Guid scheduleId, CancellationToken token)
    {
        using var command = connection.CreateCommand();

        command.CommandText = CancelSql;
        command.Transaction = transaction;

        AddParameter(command, "@ScheduleId", DbType.Guid, scheduleId);

        if (command is not DbCommand dbCommand)
        {
            throw new InvalidOperationException("The database command must support asynchronous execution.");
        }

        var affectedRows = await dbCommand.ExecuteNonQueryAsync(token);
        return affectedRows > 0;
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