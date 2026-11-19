using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.Infrastructure;
using Asyncomnia.Messaging.Sagas.Persistence.Retention;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.Sagas;

[Collection(SqlServerCollection.Name)]
public sealed class SqlServerSagaCleanupIntegrationTests : IAsyncLifetime
{
    private readonly string _connectionString;
    private IHost? _host;

    public SqlServerSagaCleanupIntegrationTests(
        SqlServerContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task Migration_CreatesFilteredSagaCleanupIndex()
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
            WHERE index_definition.[name] = N'IX_Saga_FinalizedCleanup'
              AND index_definition.[object_id] = OBJECT_ID(N'[asyncomnia].[Saga]')
              AND index_column.[key_ordinal] > 0
            ORDER BY index_column.[key_ordinal]
            """;
        const string filterSql = """
            SELECT [filter_definition]
            FROM sys.indexes
            WHERE [name] = N'IX_Saga_FinalizedCleanup'
              AND [object_id] = OBJECT_ID(N'[asyncomnia].[Saga]')
            """;
        await using var connection = new SqlConnection(_connectionString);

        var columns = (await connection.QueryAsync<string>(columnsSql)).ToArray();
        var filter = await connection.QuerySingleAsync<string>(filterSql);

        Assert.Equal(["UpdatedAt"], columns);
        Assert.Contains("[CurrentState]", filter);
        Assert.Contains("Final", filter);
    }

    [Fact]
    public async Task DeleteFinalizedAsync_DeletesOnlyFinalizedSagasOlderThanRetention()
    {
        var now = await GetDatabaseUtcNowAsync();
        var expiredFinal = CreateRow("Final", now.AddDays(-31));
        var recentFinal = CreateRow("Final", now.AddDays(-29));
        var active = CreateRow("Active", now.AddDays(-90));
        var businessFailed = CreateRow("Failed", now.AddDays(-90));
        await InsertRowsAsync([expiredFinal, recentFinal, active, businessFailed]);
        await using var scope = _host!.Services.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<ISagaCleanupStore>();

        var deletedCount = await store.DeleteFinalizedAsync(
            TimeSpan.FromDays(30),
            batchSize: 10);

        Assert.Equal(1, deletedCount);
        var remainingIds = await GetCorrelationIdsAsync();
        Assert.DoesNotContain(expiredFinal.CorrelationId, remainingIds);
        Assert.Contains(recentFinal.CorrelationId, remainingIds);
        Assert.Contains(active.CorrelationId, remainingIds);
        Assert.Contains(businessFailed.CorrelationId, remainingIds);
    }

    [Fact]
    public async Task DeleteFinalizedAsync_WhenMoreSagasExistThanBatch_DeletesOldestBatch()
    {
        var now = await GetDatabaseUtcNowAsync();
        var oldest = CreateRow("Final", now.AddDays(-40));
        var middle = CreateRow("Final", now.AddDays(-39));
        var newest = CreateRow("Final", now.AddDays(-38));
        await InsertRowsAsync([oldest, middle, newest]);
        await using var scope = _host!.Services.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<ISagaCleanupStore>();

        var deletedCount = await store.DeleteFinalizedAsync(
            TimeSpan.FromDays(30),
            batchSize: 2);

        Assert.Equal(2, deletedCount);
        var remainingIds = await GetCorrelationIdsAsync();
        Assert.DoesNotContain(oldest.CorrelationId, remainingIds);
        Assert.DoesNotContain(middle.CorrelationId, remainingIds);
        Assert.Contains(newest.CorrelationId, remainingIds);
    }

    [Fact]
    public async Task DeleteFinalizedAsync_WhenRetentionExceedsIntegerSeconds_UsesFullRetention()
    {
        var now = await GetDatabaseUtcNowAsync();
        var expired = CreateRow("Final", now.AddYears(-80));
        var retained = CreateRow("Final", now.AddYears(-60));
        await InsertRowsAsync([expired, retained]);
        await using var scope = _host!.Services.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<ISagaCleanupStore>();

        var deletedCount = await store.DeleteFinalizedAsync(
            TimeSpan.FromDays(365 * 70),
            batchSize: 10);

        Assert.Equal(1, deletedCount);
        var remainingIds = await GetCorrelationIdsAsync();
        Assert.DoesNotContain(expired.CorrelationId, remainingIds);
        Assert.Contains(retained.CorrelationId, remainingIds);
    }

    [Fact]
    public async Task DeleteFinalizedAsync_WhenSeveralInstancesRunConcurrently_DeletesEverySagaOnce()
    {
        const int workerCount = 4;
        const int batchSize = 10;
        var now = await GetDatabaseUtcNowAsync();
        var rows = Enumerable.Range(0, workerCount * batchSize)
            .Select(index => CreateRow(
                "Final",
                now.AddDays(-31).AddMinutes(-index)))
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
        Assert.Equal(0, await CountSagaRowsAsync());
    }

    public async Task InitializeAsync()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services
            .AddAsyncomniaMessaging()
            .UseSqlServer(sql => sql.UseConnectionString(_connectionString));

        _host = builder.Build();

        await _host.MigrateAsyncomniaAsync();
        await ExecuteNonQueryAsync("DELETE FROM [asyncomnia].[Saga]");
    }

    public Task DisposeAsync()
    {
        _host?.Dispose();
        return Task.CompletedTask;
    }

    private async Task<int> DeleteBatchInNewScopeAsync(int batchSize)
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<ISagaCleanupStore>();

        return await store.DeleteFinalizedAsync(
            TimeSpan.FromDays(30),
            batchSize);
    }

    private async Task InsertRowsAsync(IEnumerable<SagaCleanupRow> rows)
    {
        const string sql = """
            INSERT INTO [asyncomnia].[Saga]
            (
                [SagaType],
                [CorrelationId],
                [CurrentState],
                [DataJson],
                [CreatedAt],
                [UpdatedAt]
            )
            VALUES
            (
                @SagaType,
                @CorrelationId,
                @CurrentState,
                N'{}',
                @CreatedAt,
                @UpdatedAt
            )
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.ExecuteAsync(sql, rows);
    }

    private async Task<Guid[]> GetCorrelationIdsAsync()
    {
        await using var connection = new SqlConnection(_connectionString);
        var ids = await connection.QueryAsync<Guid>(
            "SELECT [CorrelationId] FROM [asyncomnia].[Saga]");

        return ids.ToArray();
    }

    private async Task<DateTimeOffset> GetDatabaseUtcNowAsync()
    {
        await using var connection = new SqlConnection(_connectionString);
        return await connection.QuerySingleAsync<DateTimeOffset>(
            "SELECT CAST(SYSUTCDATETIME() AS datetimeoffset(7))");
    }

    private Task<int> CountSagaRowsAsync()
    {
        return ExecuteScalarAsync<int>(
            "SELECT COUNT(*) FROM [asyncomnia].[Saga]");
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

    private static SagaCleanupRow CreateRow(
        string currentState,
        DateTimeOffset updatedAt)
    {
        return new SagaCleanupRow(
            $"Tests.Saga{Guid.NewGuid():N}, Tests",
            Guid.NewGuid(),
            currentState,
            updatedAt.AddDays(-1),
            updatedAt);
    }

    private sealed record SagaCleanupRow(
        string SagaType,
        Guid CorrelationId,
        string CurrentState,
        DateTimeOffset CreatedAt,
        DateTimeOffset UpdatedAt);
}
