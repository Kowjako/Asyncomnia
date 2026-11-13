using System.Data;
using System.Data.Common;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Microsoft.Data.SqlClient;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Inbox;

internal sealed class SqlServerInboxWriter(IDbTransactionAccessor transactionAccessor) : IInboxWriter
{
    private const string InsertSql = $@"
        INSERT INTO {SqlServerInboxDefaults.QualifiedTableName}
        (
            [MessageId],
            [ConsumerId],
            [ContractType],
            [ReceivedAt]
        )
        VALUES
        (
            @MessageId,
            @ConsumerId,
            @ContractType,
            @ReceivedAt
        )";

    private readonly IDbTransactionAccessor _transactionAccessor = transactionAccessor
       ?? throw new ArgumentNullException(nameof(transactionAccessor));

    public async Task<bool> TryAddAsync(InboxMessage message, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        token.ThrowIfCancellationRequested();

        var tx = _transactionAccessor.CurrentTransaction 
            ?? throw new InvalidOperationException("An active inbox transaction is required");

        var connection = tx.Connection 
            ?? throw new InvalidOperationException("The current inbox transaction is no longer associated with a database connection");

        if (connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException("The database connection associated with the current Inbox transaction must be open");
        }

        try
        {
            await ExecuteInsertAsync(connection, tx, message, token);
            return true;
        }
        catch (SqlException exception) when (exception.Number is 2601 or 2627)
        {
            return false;
        }
    }

    private static async Task ExecuteInsertAsync(IDbConnection connection, IDbTransaction transaction, InboxMessage message, CancellationToken token)
    {
        using var command = connection.CreateCommand();
        command.CommandText = InsertSql;
        command.Transaction = transaction;

        AddParameter(command, "@MessageId", DbType.Guid, message.MessageId);
        AddParameter(command, "@ConsumerId", DbType.String, message.ConsumerId, size: 255);
        AddParameter(command, "@ContractType", DbType.String, message.ContractType, size: 1000);
        AddParameter(command, "@ReceivedAt", DbType.DateTimeOffset, message.ReceivedAt);

        if (command is not DbCommand dbCommand)
        {
            throw new InvalidOperationException("The database command must support asynchronous execution.");
        }

        await dbCommand.ExecuteNonQueryAsync(token);
    }

    private static void AddParameter(IDbCommand command, string name, DbType type, object value, int? size = null)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = type;
        parameter.Value = value;

        if (size.HasValue)
        {
            parameter.Size = size.Value;
        }

        command.Parameters.Add(parameter);
    }
}
