using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.Infrastructure;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.Outbox;

[Collection(SqlServerCollection.Name)]
public sealed class SqlServerOutboxCleanupIntegrationTests : IAsyncLifetime
{
    private readonly string _connectionString;
    private IHost? _host;

    public SqlServerOutboxCleanupIntegrationTests(
        SqlServerContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Theory]
    [InlineData("IX_Outbox_ProcessedAt", "ProcessedAt")]
    [InlineData("IX_Outbox_FailedAt", "FailedAt")]
    public async Task Migration_CreatesOutboxCleanupIndexWithExpectedColumnsAndFilter(
        string indexName,
        string terminalColumn)
    {
        const string columnsSql = """
            SELECT column_definition.[name]
            FROM sys.indexes AS index_definition
            INNER JOIN sys.index_columns AS index_column
                ON index_column.[object_id] = index_definition.[object_id]
               AND index_column.[index_id] = index_definition.[index_id]
            INNER JOIN sys.columns AS column_definition
                ON column_definition.[object_id] = index_column.[object_id]
               AND column_definition.[column_id] = index_column.[column_id]
            WHERE index_definition.[name] = @IndexName
              AND index_definition.[object_id] = OBJECT_ID(N'[asyncomnia].[Outbox]')
              AND index_column.[key_ordinal] > 0
            ORDER BY index_column.[key_ordinal]
            """;
        const string filterSql = """
            SELECT [filter_definition]
            FROM sys.indexes
            WHERE [name] = @IndexName
              AND [object_id] = OBJECT_ID(N'[asyncomnia].[Outbox]')
            """;
        await using var connection = new SqlConnection(_connectionString);

        var columns = (await connection.QueryAsync<string>(
            columnsSql,
            new { IndexName = indexName })).ToArray();
        var filter = await connection.QuerySingleAsync<string>(
            filterSql,
            new { IndexName = indexName });

        Assert.Equal([terminalColumn, "MessageId"], columns);
        Assert.Contains($"[{terminalColumn}] IS NOT NULL", filter);
    }

    [Fact]
    public async Task DeleteExpiredAsync_DeletesOnlyExpiredProcessedMessagesByDefault()
    {
        var now = await GetDatabaseUtcNowAsync();
        var expiredProcessed = CreateRow(
            processedAt: now.AddDays(-8));
        var recentProcessed = CreateRow(
            processedAt: now.AddDays(-6));
        var expiredFailed = CreateRow(
            failedAt: now.AddDays(-40));
        var pending = CreateRow(
            createdAt: now.AddDays(-100));
        var leased = CreateRow(
            createdAt: now.AddDays(-100),
            leaseId: Guid.NewGuid(),
            leaseExpiresAt: now.AddMinutes(1));
        await InsertRowsAsync(
        [
            expiredProcessed,
            recentProcessed,
            expiredFailed,
            pending,
            leased
        ]);
        await using var scope = _host!.Services.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IOutboxCleanupStore>();

        var deletedCount = await store.DeleteExpiredAsync(
            TimeSpan.FromDays(7),
            failedMessageRetention: null,
            batchSize: 10);

        Assert.Equal(1, deletedCount);
        var remainingIds = await GetMessageIdsAsync();
        Assert.DoesNotContain(expiredProcessed.MessageId, remainingIds);
        Assert.Contains(recentProcessed.MessageId, remainingIds);
        Assert.Contains(expiredFailed.MessageId, remainingIds);
        Assert.Contains(pending.MessageId, remainingIds);
        Assert.Contains(leased.MessageId, remainingIds);
    }

    [Fact]
    public async Task DeleteExpiredAsync_WithFailedRetention_DeletesExpiredProcessedAndFailedMessages()
    {
        var now = await GetDatabaseUtcNowAsync();
        var expiredFailed = CreateRow(
            failedAt: now.AddDays(-31));
        var recentFailed = CreateRow(
            failedAt: now.AddDays(-29));
        var expiredProcessed = CreateRow(
            processedAt: now.AddDays(-8));
        var recentProcessed = CreateRow(
            processedAt: now.AddDays(-1));
        await InsertRowsAsync(
        [
            expiredFailed,
            recentFailed,
            expiredProcessed,
            recentProcessed
        ]);
        await using var scope = _host!.Services.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IOutboxCleanupStore>();

        var deletedCount = await store.DeleteExpiredAsync(
            TimeSpan.FromDays(7),
            TimeSpan.FromDays(30),
            batchSize: 10);

        Assert.Equal(2, deletedCount);
        var remainingIds = await GetMessageIdsAsync();
        Assert.DoesNotContain(expiredFailed.MessageId, remainingIds);
        Assert.DoesNotContain(expiredProcessed.MessageId, remainingIds);
        Assert.Contains(recentFailed.MessageId, remainingIds);
        Assert.Contains(recentProcessed.MessageId, remainingIds);
    }

    [Fact]
    public async Task DeleteExpiredAsync_WhenMoreMessagesExistThanBatch_DeletesOldestTerminalMessages()
    {
        var now = await GetDatabaseUtcNowAsync();
        var oldest = CreateRow(processedAt: now.AddDays(-10));
        var middle = CreateRow(processedAt: now.AddDays(-9));
        var newest = CreateRow(processedAt: now.AddDays(-8));
        await InsertRowsAsync([oldest, middle, newest]);
        await using var scope = _host!.Services.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IOutboxCleanupStore>();

        var deletedCount = await store.DeleteExpiredAsync(
            TimeSpan.FromDays(7),
            failedMessageRetention: null,
            batchSize: 2);

        Assert.Equal(2, deletedCount);
        var remainingIds = await GetMessageIdsAsync();
        Assert.DoesNotContain(oldest.MessageId, remainingIds);
        Assert.DoesNotContain(middle.MessageId, remainingIds);
        Assert.Contains(newest.MessageId, remainingIds);
    }

    [Fact]
    public async Task DeleteExpiredAsync_WhenSeveralInstancesRunConcurrently_DeletesEveryMessageOnce()
    {
        const int workerCount = 4;
        const int batchSize = 10;
        var now = await GetDatabaseUtcNowAsync();
        var rows = Enumerable.Range(0, workerCount * batchSize)
            .Select(index => CreateRow(
                processedAt: now.AddDays(-8).AddMinutes(-index)))
            .ToArray();
        await InsertRowsAsync(rows);

        var cleanupTasks = Enumerable.Range(0, workerCount)
            .Select(_ => DeleteBatchInNewScopeAsync(batchSize))
            .ToArray();
        var deletedCounts = await Task.WhenAll(cleanupTasks);
        var totalDeleted = deletedCounts.Sum();

        while (true)
        {
            var deletedCount = await DeleteBatchInNewScopeAsync(batchSize);

            if (deletedCount == 0)
            {
                break;
            }

            totalDeleted += deletedCount;
        }

        Assert.Equal(rows.Length, totalDeleted);
        Assert.Equal(0, await CountOutboxMessagesAsync());
    }

    public async Task InitializeAsync()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services
            .AddAsyncomniaMessaging()
            .UseSqlServer(sql => sql.UseConnectionString(_connectionString));

        _host = builder.Build();

        await _host.MigrateAsyncomniaAsync();
        await ExecuteNonQueryAsync("DELETE FROM [asyncomnia].[Outbox]");
    }

    public Task DisposeAsync()
    {
        _host?.Dispose();
        return Task.CompletedTask;
    }

    private async Task<int> DeleteBatchInNewScopeAsync(int batchSize)
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IOutboxCleanupStore>();

        return await store.DeleteExpiredAsync(
            TimeSpan.FromDays(7),
            failedMessageRetention: null,
            batchSize: batchSize);
    }

    private async Task InsertRowsAsync(IEnumerable<OutboxRow> rows)
    {
        const string sql = """
            INSERT INTO [asyncomnia].[Outbox]
            (
                [MessageId],
                [ContractType],
                [MessageJson],
                [CreatedAt],
                [AvailableAt],
                [LeaseId],
                [LeaseExpiresAt],
                [ProcessedAt],
                [FailedAt]
            )
            VALUES
            (
                @MessageId,
                N'Tests.OutboxMessage, Tests',
                N'{}',
                @CreatedAt,
                @CreatedAt,
                @LeaseId,
                @LeaseExpiresAt,
                @ProcessedAt,
                @FailedAt
            )
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.ExecuteAsync(sql, rows);
    }

    private async Task<Guid[]> GetMessageIdsAsync()
    {
        await using var connection = new SqlConnection(_connectionString);
        var ids = await connection.QueryAsync<Guid>(
            "SELECT [MessageId] FROM [asyncomnia].[Outbox]");

        return ids.ToArray();
    }

    private async Task<DateTimeOffset> GetDatabaseUtcNowAsync()
    {
        await using var connection = new SqlConnection(_connectionString);
        return await connection.QuerySingleAsync<DateTimeOffset>(
            "SELECT CAST(SYSUTCDATETIME() AS datetimeoffset(7))");
    }

    private Task<int> CountOutboxMessagesAsync()
    {
        return ExecuteScalarAsync<int>(
            "SELECT COUNT(*) FROM [asyncomnia].[Outbox]");
    }

    private async Task<T?> ExecuteScalarAsync<T>(string sql)
    {
        await using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<T>(sql);
    }

    private async Task ExecuteNonQueryAsync(string sql)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.ExecuteAsync(sql);
    }

    private static OutboxRow CreateRow(
        DateTimeOffset? createdAt = null,
        DateTimeOffset? processedAt = null,
        DateTimeOffset? failedAt = null,
        Guid? leaseId = null,
        DateTimeOffset? leaseExpiresAt = null)
    {
        return new OutboxRow(
            Guid.NewGuid(),
            createdAt ?? DateTimeOffset.UtcNow,
            processedAt,
            failedAt,
            leaseId,
            leaseExpiresAt);
    }

    private sealed record OutboxRow(
        Guid MessageId,
        DateTimeOffset CreatedAt,
        DateTimeOffset? ProcessedAt,
        DateTimeOffset? FailedAt,
        Guid? LeaseId,
        DateTimeOffset? LeaseExpiresAt);
}
