using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Inbox.Storage;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.Infrastructure;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.Inbox;

[Collection(SqlServerCollection.Name)]
public sealed class SqlServerInboxCleanupIntegrationTests : IAsyncLifetime
{
    private readonly string _connectionString;
    private IHost? _host;

    public SqlServerInboxCleanupIntegrationTests(SqlServerContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task Migration_CreatesInboxCleanupIndexWithExpectedColumns()
    {
        const string sql = """
            SELECT column_definition.[name]
            FROM sys.indexes AS index_definition
            INNER JOIN sys.index_columns AS index_column
                ON index_column.[object_id] = index_definition.[object_id]
               AND index_column.[index_id] = index_definition.[index_id]
            INNER JOIN sys.columns AS column_definition
                ON column_definition.[object_id] = index_column.[object_id]
               AND column_definition.[column_id] = index_column.[column_id]
            WHERE index_definition.[name] = N'IX_Inbox_ReceivedAt'
              AND index_definition.[object_id] = OBJECT_ID(N'[asyncomnia].[Inbox]')
              AND index_column.[key_ordinal] > 0
            ORDER BY index_column.[key_ordinal]
            """;

        await using var connection = new SqlConnection(_connectionString);
        var columns = (await connection.QueryAsync<string>(sql)).ToArray();

        Assert.Equal(["ReceivedAt", "ConsumerId", "MessageId"], columns);
    }

    [Fact]
    public async Task DeleteExpiredAsync_DeletesOnlyMessagesOlderThanCutoff()
    {
        var cutoff = new DateTimeOffset(2026, 11, 13, 12, 0, 0, TimeSpan.Zero);
        var expiredId = Guid.NewGuid();
        var boundaryId = Guid.NewGuid();
        var recentId = Guid.NewGuid();
        await InsertMessagesAsync(
        [
            new InboxRow(expiredId, "service", cutoff.AddTicks(-1)),
            new InboxRow(boundaryId, "service", cutoff),
            new InboxRow(recentId, "service", cutoff.AddMinutes(1))
        ]);
        await using var scope = _host!.Services.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IInboxCleanupStore>();

        var deletedCount = await store.DeleteExpiredAsync(cutoff, batchSize: 10);

        Assert.Equal(1, deletedCount);
        var remainingIds = await GetMessageIdsAsync();
        Assert.DoesNotContain(expiredId, remainingIds);
        Assert.Contains(boundaryId, remainingIds);
        Assert.Contains(recentId, remainingIds);
    }

    [Fact]
    public async Task DeleteExpiredAsync_WhenMoreMessagesExistThanBatch_DeletesOldestBatch()
    {
        var cutoff = new DateTimeOffset(2026, 11, 13, 12, 0, 0, TimeSpan.Zero);
        var oldestId = Guid.NewGuid();
        var middleId = Guid.NewGuid();
        var newestId = Guid.NewGuid();
        await InsertMessagesAsync(
        [
            new InboxRow(oldestId, "service", cutoff.AddHours(-3)),
            new InboxRow(middleId, "service", cutoff.AddHours(-2)),
            new InboxRow(newestId, "service", cutoff.AddHours(-1))
        ]);
        await using var scope = _host!.Services.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IInboxCleanupStore>();

        var deletedCount = await store.DeleteExpiredAsync(cutoff, batchSize: 2);

        Assert.Equal(2, deletedCount);
        var remainingIds = await GetMessageIdsAsync();
        Assert.DoesNotContain(oldestId, remainingIds);
        Assert.DoesNotContain(middleId, remainingIds);
        Assert.Contains(newestId, remainingIds);
    }

    [Fact]
    public async Task DeleteExpiredAsync_WhenSeveralInstancesRunConcurrently_DoesNotDeleteMessageTwice()
    {
        const int workerCount = 4;
        const int batchSize = 10;
        var cutoff = new DateTimeOffset(2026, 11, 13, 12, 0, 0, TimeSpan.Zero);
        var messages = Enumerable.Range(0, workerCount * batchSize)
            .Select(index => new InboxRow(
                Guid.NewGuid(),
                "service",
                cutoff.AddMinutes(-(index + 1))))
            .ToArray();
        await InsertMessagesAsync(messages);

        var cleanupTasks = Enumerable.Range(0, workerCount)
            .Select(_ => DeleteBatchInNewScopeAsync(cutoff, batchSize))
            .ToArray();
        var deletedCounts = await Task.WhenAll(cleanupTasks);
        var totalDeleted = deletedCounts.Sum();

        while (true)
        {
            var deletedCount = await DeleteBatchInNewScopeAsync(cutoff, batchSize);

            if (deletedCount == 0)
            {
                break;
            }

            totalDeleted += deletedCount;
        }

        Assert.Equal(messages.Length, totalDeleted);
        Assert.Equal(0, await CountInboxMessagesAsync());
    }

    public async Task InitializeAsync()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services
            .AddAsyncomniaMessaging()
            .UseSqlServer(options =>
                options.ConnectionString = _connectionString);

        _host = builder.Build();

        await _host.MigrateAsyncomniaAsync();
        await ExecuteNonQueryAsync("DELETE FROM [asyncomnia].[Inbox]");
    }

    public Task DisposeAsync()
    {
        _host?.Dispose();
        return Task.CompletedTask;
    }

    private async Task<int> DeleteBatchInNewScopeAsync(
        DateTimeOffset cutoff,
        int batchSize)
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IInboxCleanupStore>();

        return await store.DeleteExpiredAsync(cutoff, batchSize);
    }

    private async Task InsertMessagesAsync(IEnumerable<InboxRow> messages)
    {
        const string sql = """
            INSERT INTO [asyncomnia].[Inbox]
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
                N'Tests.InboxMessage, Tests',
                @ReceivedAt
            )
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.ExecuteAsync(sql, messages);
    }

    private async Task<Guid[]> GetMessageIdsAsync()
    {
        await using var connection = new SqlConnection(_connectionString);
        var ids = await connection.QueryAsync<Guid>(
            "SELECT [MessageId] FROM [asyncomnia].[Inbox]");

        return ids.ToArray();
    }

    private Task<int> CountInboxMessagesAsync()
    {
        return ExecuteScalarAsync<int>(
            "SELECT COUNT(*) FROM [asyncomnia].[Inbox]");
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

    private sealed record InboxRow(
        Guid MessageId,
        string ConsumerId,
        DateTimeOffset ReceivedAt);
}
