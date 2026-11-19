using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Infrastructure.Connection;
using Asyncomnia.Messaging.Sagas.Persistence.Retention;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Sagas.Retention;

internal sealed class SqlServerSagaCleanupStore(ISqlServerConnectionFactory connectionFactory) : ISagaCleanupStore
{
    private const string DeleteFinalizedSql = $@"
        DECLARE @Now datetimeoffset(7) = SYSUTCDATETIME();
        DECLARE @FinalizedBefore datetimeoffset(7) =
            DATEADD(SECOND, -@RetentionSeconds, DATEADD(DAY, -@RetentionDays, @Now));

        ;WITH FinalizedSagas AS
        (
            SELECT TOP (@BatchSize) [SagaType], [CorrelationId]
            FROM {SqlServerSagaDefaults.QualifiedTableName} WITH (UPDLOCK, READPAST, READCOMMITTEDLOCK, ROWLOCK)
            WHERE [CurrentState] = N'Final' AND [UpdatedAt] < @FinalizedBefore
            ORDER BY [UpdatedAt], [SagaType], [CorrelationId]
        )
        DELETE FROM FinalizedSagas;

        SELECT @@ROWCOUNT";

    private readonly ISqlServerConnectionFactory _connectionFactory = connectionFactory
        ?? throw new ArgumentNullException(nameof(connectionFactory));

    public async Task<int> DeleteFinalizedAsync(TimeSpan finalizedSagaRetention, int batchSize, CancellationToken token = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(finalizedSagaRetention,TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);

        var (retentionDays, retentionSeconds) = SplitRetention(finalizedSagaRetention);

        await using var connection = await _connectionFactory.OpenConnectionAsync(token);

        await using var command = connection.CreateCommand();
        command.CommandText = DeleteFinalizedSql;
        command.Parameters.Add("@RetentionDays", System.Data.SqlDbType.Int).Value = retentionDays;
        command.Parameters.Add("@RetentionSeconds", System.Data.SqlDbType.Int).Value = retentionSeconds;
        command.Parameters.Add("@BatchSize", System.Data.SqlDbType.Int).Value = batchSize;

        var result = await command.ExecuteScalarAsync(token);

        return result is int deletedCount
            ? deletedCount
            : throw new InvalidOperationException("SQL Server returned an invalid Saga cleanup result.");
    }

    private static (int Days, int Seconds) SplitRetention(TimeSpan retention)
    {
        var days = checked((int)Math.Floor(retention.TotalDays));
        var remainder = retention - TimeSpan.FromDays(days);
        var seconds = checked((int)Math.Ceiling(remainder.TotalSeconds));

        if (seconds == 86_400)
        {
            days = checked(days + 1);
            seconds = 0;
        }

        return (days, seconds);
    }
}
