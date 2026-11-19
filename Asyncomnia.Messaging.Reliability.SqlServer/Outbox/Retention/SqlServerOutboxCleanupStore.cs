using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Asyncomnia.Messaging.Reliability.SqlServer.Infrastructure.Connection;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Outbox.Retention;

internal sealed class SqlServerOutboxCleanupStore(ISqlServerConnectionFactory connectionFactory) : IOutboxCleanupStore
{
    private readonly ISqlServerConnectionFactory _connectionFactory = connectionFactory
        ?? throw new ArgumentNullException(nameof(connectionFactory));

    private const string sql = @$"
        DECLARE @Now datetimeoffset(7) = SYSUTCDATETIME();
        DECLARE @ProcessedBefore datetimeoffset(7) = DATEADD(SECOND, -@ProcessedRetentionSeconds, @Now);
        DECLARE @FailedBefore datetimeoffset(7) = NULL;
        
        IF @FailedRetentionSeconds IS NOT NULL
        BEGIN
            SET @FailedBefore = DATEADD(SECOND, -@FailedRetentionSeconds, @Now);
        END;
        
        ;WITH ExpiredOutboxMessages AS
        (
            SELECT TOP (@BatchSize) [MessageId]
            FROM [asyncomnia].[Outbox] WITH (UPDLOCK, READPAST, READCOMMITTEDLOCK, ROWLOCK)
            WHERE (
             [ProcessedAt] IS NOT NULL AND [ProcessedAt] < @ProcessedBefore
            ) OR (
             @FailedBefore IS NOT NULL AND [FailedAt] IS NOT NULL AND [FailedAt] < @FailedBefore
            )
            ORDER BY COALESCE([ProcessedAt], [FailedAt]), [MessageId]
        )
        DELETE FROM ExpiredOutboxMessages;
        SELECT @@ROWCOUNT";

    public async Task<int> DeleteExpiredAsync(TimeSpan processedMessageRetention, TimeSpan? failedMessageRetention, int batchSize, CancellationToken token = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(processedMessageRetention, TimeSpan.Zero);

        if (failedMessageRetention != null && failedMessageRetention <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(failedMessageRetention));
        }

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);

        var processedRetentionSeconds = checked((int)Math.Ceiling(processedMessageRetention.TotalSeconds));

        var failedRetentionSeconds = failedMessageRetention.HasValue
            ? checked((int)Math.Ceiling(failedMessageRetention.Value.TotalSeconds))
            : (int?)null;

        await using var connection = await _connectionFactory.OpenConnectionAsync(token);

        await using var command = connection.CreateCommand();

        command.CommandText = sql;

        command.Parameters.Add("@FailedRetentionSeconds", System.Data.SqlDbType.Int).Value = 
            failedRetentionSeconds.HasValue ? failedRetentionSeconds.Value : DBNull.Value;

        command.Parameters.Add("@BatchSize", System.Data.SqlDbType.Int).Value = batchSize;
        command.Parameters.Add("@ProcessedRetentionSeconds", System.Data.SqlDbType.Int).Value = processedRetentionSeconds;

        var result = await command.ExecuteScalarAsync(token);

        return result is int deletedCount
            ? deletedCount
            : throw new InvalidOperationException("SQL Server returned an invalid Outbox cleanup result.");
    }
}
