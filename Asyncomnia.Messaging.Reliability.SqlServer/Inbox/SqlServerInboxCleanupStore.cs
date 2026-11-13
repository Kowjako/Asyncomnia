using Asyncomnia.Messaging.Reliability.Inbox.Storage;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Inbox;

internal sealed class SqlServerInboxCleanupStore(IOptions<SqlServerOptions> options) : IInboxCleanupStore
{
    private readonly string _connectionString = options?.Value.ConnectionString
        ?? throw new ArgumentNullException(nameof(options));

    public async Task<int> DeleteExpiredAsync(DateTimeOffset expiresBefore, int batchSize, CancellationToken token = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);

        var sql = $@"
            ;WITH ExpiredInboxMessages AS
            (
                SELECT TOP (@BatchSize)
                    [ConsumerId],
                    [MessageId]
                FROM {SqlServerInboxDefaults.QualifiedTableName} WITH (UPDLOCK, READPAST, READCOMMITTEDLOCK)
                WHERE [ReceivedAt] < @ExpiresBefore
                ORDER BY [ReceivedAt], [ConsumerId], [MessageId]
            )
            DELETE FROM ExpiredInboxMessages;

            SELECT @@ROWCOUNT";

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(token);

        await using var command = connection.CreateCommand();

        command.CommandText = sql;
        command.Parameters.Add("@BatchSize", System.Data.SqlDbType.Int).Value = batchSize;
        command.Parameters.Add("@ExpiresBefore", System.Data.SqlDbType.DateTimeOffset).Value = expiresBefore;

        var result = await command.ExecuteScalarAsync(token);

        return result is int deletedCount
            ? deletedCount
            : throw new InvalidOperationException("SQL Server returned an invalid Inbox cleanup result.");
    }
}