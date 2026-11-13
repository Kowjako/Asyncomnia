using System.Data;
using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.DelayedPublishing;

internal sealed class SqlServerDelayedMessageStore(IOptions<SqlServerOptions> options) : IDelayedMessageStore
{
    private readonly string _connectionString = options?.Value.ConnectionString
        ?? throw new ArgumentNullException(nameof(options));

    private const string MoveDueSql = $@"
        DECLARE @OutboxAvailableAt datetimeoffset(7) = SYSUTCDATETIME();

        DECLARE @DueMessages TABLE
        (
            [MessageId] uniqueidentifier NOT NULL,
            [ContractType] nvarchar(1000) NOT NULL,
            [MessageJson] nvarchar(max) NOT NULL,
            [CorrelationId] uniqueidentifier NULL,
            [CreatedAt] datetimeoffset(7) NOT NULL
        );

        ;WITH DueMessages AS
        (
            SELECT TOP (@BatchSize)
                [ScheduleId],
                [MessageId],
                [ContractType],
                [MessageJson],
                [CorrelationId],
                [CreatedAt],
                [DeliverAt]
            FROM {SqlServerDelayedDefaults.QualifiedTableName} WITH (UPDLOCK, READPAST, READCOMMITTEDLOCK)
            WHERE [DeliverAt] <= @UtcNow
            ORDER BY [DeliverAt], [ScheduleId]
        )
        DELETE FROM DueMessages
        OUTPUT
            DELETED.[MessageId],
            DELETED.[ContractType],
            DELETED.[MessageJson],
            DELETED.[CorrelationId],
            DELETED.[CreatedAt]
        INTO @DueMessages
        (
            [MessageId],
            [ContractType],
            [MessageJson],
            [CorrelationId],
            [CreatedAt]
        );

        INSERT INTO {SqlServerOutboxDefaults.QualifiedTableName}
        (
            [MessageId],
            [ContractType],
            [MessageJson],
            [CorrelationId],
            [CreatedAt],
            [AvailableAt]
        )
        SELECT
            [MessageId],
            [ContractType],
            [MessageJson],
            [CorrelationId],
            [CreatedAt],
            @OutboxAvailableAt
        FROM @DueMessages;

        SELECT COUNT(*) FROM @DueMessages";

    public async Task<int> MoveDueToOutboxAsync(DateTimeOffset utcNow, int batchSize, CancellationToken token = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token);
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(token);

        try
        {
            await using var command = connection.CreateCommand();

            command.Transaction = transaction;
            command.CommandText = MoveDueSql;

            command.Parameters.Add("@BatchSize", SqlDbType.Int).Value = batchSize;
            command.Parameters.Add("@UtcNow", SqlDbType.DateTimeOffset).Value = utcNow;

            var result = await command.ExecuteScalarAsync(token);

            var movedCount = result is int count
                ? count
                : throw new InvalidOperationException("SQL Server returned an invalid delayed message result.");

            await transaction.CommitAsync(token);

            return movedCount;
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }
}
