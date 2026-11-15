using System.Data;
using System.Data.Common;
using System.Text.Json;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Asyncomnia.Messaging.Sagas.Persistence;
using Asyncomnia.Messaging.Sagas.StateMachine;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Sagas;

internal class SqlServerSagaRepository<TSagaInstance>(IDbTransactionAccessor dbTransactionAccessor) 
    : ISagaRepository<TSagaInstance> where TSagaInstance : AsyncomniaSagaInstance
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private static readonly string SagaType = ResolveSagaType();

    private readonly IDbTransactionAccessor _transactionAccessor = dbTransactionAccessor
        ?? throw new ArgumentNullException(nameof(dbTransactionAccessor));

    private const string SelectForUpdateSql = $@"
        SELECT [CurrentState], [DataJson]
        FROM {SqlServerSagaDefaults.QualifiedTableName} WITH (UPDLOCK, ROWLOCK)
        WHERE [SagaType] = @SagaType AND [CorrelationId] = @CorrelationId";

    public async Task<TSagaInstance?> GetForUpdateAsync(Guid correlationId, CancellationToken token)
    {
        if (correlationId == Guid.Empty)
        {
            throw new ArgumentException("Correlation ID cannot be empty", nameof(correlationId));
        }

        token.ThrowIfCancellationRequested();

        var (connection, transaction) = GetActiveTransaction();

        using var command = connection.CreateCommand();
        command.CommandText = SelectForUpdateSql;
        command.Transaction = transaction;

        AddParameter(command, "@SagaType", DbType.String, SagaType, 1000);
        AddParameter(command, "@CorrelationId", DbType.Guid, correlationId);

        if (command is not DbCommand dbCommand)
        {
            throw new InvalidOperationException("The database command must support asynchronous execution.");
        }

        await using var reader = await dbCommand.ExecuteReaderAsync(token);

        if (!await reader.ReadAsync(token))
        {
            return null;
        }

        var currentState = reader.GetString(0);
        var dataJson = reader.GetString(1);

        var instance = JsonSerializer.Deserialize<TSagaInstance>(dataJson, SerializerOptions)
            ?? throw new InvalidOperationException($"Could not deserialize saga instance '{SagaType}'.");

        // Db saved values are source of truth
        instance.CorrelationId = correlationId;
        instance.CurrentState = currentState;

        return instance;
    }

    public Task<bool> TryInsertAsync(TSagaInstance instance, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    public Task UpdateAsync(TSagaInstance instance, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    private (IDbConnection Connection, IDbTransaction Transaction) GetActiveTransaction()
    {
        var transaction = _transactionAccessor.CurrentTransaction
            ?? throw new InvalidOperationException("An active saga transaction is required.");

        var connection = transaction.Connection
            ?? throw new InvalidOperationException("The current saga transaction is no longer associated with a database connection.");

        if (connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException("The database connection associated with the current saga transaction must be open.");
        }

        return (connection, transaction);
    }

    private static string ResolveSagaType()
    {
        var type = typeof(TSagaInstance);

        var typeName = type.FullName
            ?? throw new InvalidOperationException($"Saga type '{type}' does not have a full name.");

        var assemblyName = type.Assembly.GetName().Name
            ?? throw new InvalidOperationException($"Saga type '{type}' does not have an assembly name.");

        return $"{typeName}, {assemblyName}";
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
