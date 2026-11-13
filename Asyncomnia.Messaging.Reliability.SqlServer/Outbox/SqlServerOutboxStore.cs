using System.Data;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Outbox;

internal sealed class SqlServerOutboxStore(IOptions<SqlServerOptions> options) : IOutboxStore
{
    private readonly SqlServerOptions _options = options.Value;

    public async Task<IReadOnlyList<ClaimedOutboxMessage>> ClaimAsync(int batchSize, TimeSpan leaseDuration, CancellationToken token = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(leaseDuration, TimeSpan.Zero);

        var leaseId = Guid.NewGuid();
        var leaseDurationSeconds = checked((int)Math.Ceiling(leaseDuration.TotalSeconds));

        // Using UPDATE on CTE we dont need implicit transaction - code below is atomic
        var sql = @$"
            DECLARE @Now datetimeoffset(7) = SYSUTCDATETIME();

            ;WITH MessagesToClaim AS
            (
                SELECT TOP (@BatchSize)
                    MessageId,
                    ContractType,
                    MessageJson,
                    CorrelationId,
                    CreatedAt,
                    AttemptNumber,
                    AvailableAt,
                    LeaseId,
                    LeaseExpiresAt,
                    ProcessedAt,
                    FailedAt,
                    LastError
                FROM {SqlServerOutboxDefaults.QualifiedTableName} WITH (UPDLOCK, READPAST, READCOMMITTEDLOCK)
                WHERE ProcessedAt IS NULL
                  AND FailedAt IS NULL
                  AND AvailableAt <= @Now
                  AND
                  (
                      LeaseExpiresAt IS NULL
                      OR LeaseExpiresAt <= @Now
                  )
                ORDER BY AvailableAt, CreatedAt, MessageId
            )
            UPDATE MessagesToClaim
            SET LeaseId = @LeaseId,
                LeaseExpiresAt = DATEADD(SECOND, @LeaseDurationSeconds, @Now),
                AttemptNumber = AttemptNumber + 1
            OUTPUT
                inserted.MessageId,
                inserted.ContractType,
                inserted.MessageJson,
                inserted.CorrelationId,
                inserted.CreatedAt,
                inserted.LeaseId,
                inserted.AttemptNumber,
                inserted.LeaseExpiresAt";

        await using var connection = new SqlConnection(_options.ConnectionString);
        await connection.OpenAsync(token);

        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        command.Parameters.Add("@BatchSize", SqlDbType.Int).Value = batchSize;
        command.Parameters.Add("@LeaseId", SqlDbType.UniqueIdentifier).Value = leaseId;
        command.Parameters.Add("@LeaseDurationSeconds", SqlDbType.Int).Value = leaseDurationSeconds;

        await using var reader = await command.ExecuteReaderAsync(token);

        var messages = new List<ClaimedOutboxMessage>(batchSize);

        while (await reader.ReadAsync(token))
        {
            var messageId = reader.GetGuid(0);
            var contractType = reader.GetString(1);
            var messageJson = reader.GetString(2);
            var correlationId = reader.IsDBNull(3) ? (Guid?)null : reader.GetGuid(3);
            var createdAt = reader.GetFieldValue<DateTimeOffset>(4);
            var claimedLeaseId = reader.GetGuid(5);
            var attemptNumber = reader.GetInt32(6);
            var leaseExpiresAt = reader.GetFieldValue<DateTimeOffset>(7);

            var outboxMessage = new OutboxMessage(messageId, contractType, messageJson, correlationId, createdAt);
            messages.Add(new ClaimedOutboxMessage(outboxMessage, claimedLeaseId, attemptNumber, leaseExpiresAt));
        }

        return messages;
    }

    public async Task<bool> CompleteAsync(Guid messageId, Guid leaseId, CancellationToken token = default)
    {
        var sql = @$"
            UPDATE {SqlServerOutboxDefaults.QualifiedTableName}
            SET ProcessedAt = SYSUTCDATETIME(),
                LeaseId = NULL,
                LeaseExpiresAt = NULL
            WHERE MessageId = @MessageId AND
                  LeaseId = @LeaseId AND
                  ProcessedAt IS NULL AND
                  FailedAt IS NULL";

        await using var connection = new SqlConnection(_options.ConnectionString);
        await connection.OpenAsync(token);

        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        command.Parameters.Add("@MessageId", SqlDbType.UniqueIdentifier).Value = messageId;
        command.Parameters.Add("@LeaseId", SqlDbType.UniqueIdentifier).Value = leaseId;

        var affectedRows = await command.ExecuteNonQueryAsync(token);
        return affectedRows == 1;
    }

    public async Task<bool> FailAsync(Guid messageId, Guid leaseId, string error, CancellationToken token = default)
    {
        var storedError = NormalizeError(error);

        var sql = @$"
            UPDATE {SqlServerOutboxDefaults.QualifiedTableName}
            SET FailedAt = SYSUTCDATETIME(),
                LastError = @Error,
                LeaseId = NULL,
                LeaseExpiresAt = NULL
            WHERE MessageId = @MessageId AND 
                  LeaseId = @LeaseId AND 
                  ProcessedAt IS NULL AND 
                  FailedAt IS NULL";

        await using var connection = new SqlConnection(_options.ConnectionString);
        await connection.OpenAsync(token);

        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        command.Parameters.Add("@MessageId", SqlDbType.UniqueIdentifier).Value = messageId;
        command.Parameters.Add("@LeaseId", SqlDbType.UniqueIdentifier).Value = leaseId;
        command.Parameters.Add("@Error", SqlDbType.NVarChar, 4000).Value = storedError;

        var affectedRows = await command.ExecuteNonQueryAsync(token);
        return affectedRows == 1;
    }

    public async Task<bool> ScheduleRetryAsync(Guid messageId, Guid leaseId, string error, TimeSpan retryDelay, CancellationToken token = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(retryDelay, TimeSpan.Zero);

        var storedError = NormalizeError(error);
        var retryDelaySeconds = checked((int)Math.Ceiling(retryDelay.TotalSeconds));

        var sql = @$"
            UPDATE {SqlServerOutboxDefaults.QualifiedTableName}
            SET AvailableAt = DATEADD(SECOND, @RetryDelaySeconds, SYSUTCDATETIME()),
                LastError = @Error,
                LeaseId = NULL,
                LeaseExpiresAt = NULL
            WHERE MessageId = @MessageId AND 
                  LeaseId = @LeaseId AND
                  ProcessedAt IS NULL
                  AND FailedAt IS NULL";

        await using var connection = new SqlConnection(_options.ConnectionString);
        await connection.OpenAsync(token);

        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        command.Parameters.Add("@MessageId", SqlDbType.UniqueIdentifier).Value = messageId;
        command.Parameters.Add("@LeaseId", SqlDbType.UniqueIdentifier).Value = leaseId;
        command.Parameters.Add("@RetryDelaySeconds", SqlDbType.Int).Value = retryDelaySeconds;
        command.Parameters.Add("@Error", SqlDbType.NVarChar, 4000).Value = storedError;

        var affectedRows = await command.ExecuteNonQueryAsync(token);
        return affectedRows == 1;
    }

    private static string NormalizeError(string error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return error.Length <= 4000 ? error : error[..4000];
    }
}
