using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.Infrastructure;
using Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.TestDoubles;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.DelayedPublishing;

[Collection(SqlServerCollection.Name)]
public sealed class SqlServerDelayedPublishingIntegrationTests : IAsyncLifetime
{
    private static readonly DateTimeOffset UtcNow =
        new(2026, 11, 15, 10, 0, 0, TimeSpan.Zero);

    private readonly string _connectionString;
    private IHost? _host;

    public SqlServerDelayedPublishingIntegrationTests(
        SqlServerContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task Migration_CreatesDelayedTableAndDueIndex()
    {
        var objectId = await ExecuteScalarAsync<int?>(
            "SELECT OBJECT_ID(N'[asyncomnia].[Delayed]', N'U')");

        const string indexSql = """
            SELECT column_definition.[name]
            FROM sys.indexes AS index_definition
            INNER JOIN sys.index_columns AS index_column
                ON index_column.[object_id] = index_definition.[object_id]
               AND index_column.[index_id] = index_definition.[index_id]
            INNER JOIN sys.columns AS column_definition
                ON column_definition.[object_id] = index_column.[object_id]
               AND column_definition.[column_id] = index_column.[column_id]
            WHERE index_definition.[name] = N'IX_Delayed_Due'
              AND index_definition.[object_id] = OBJECT_ID(N'[asyncomnia].[Delayed]')
              AND index_column.[key_ordinal] > 0
            ORDER BY index_column.[key_ordinal]
            """;
        await using var connection = new SqlConnection(_connectionString);
        var indexColumns = (await connection.QueryAsync<string>(indexSql)).ToArray();

        Assert.NotNull(objectId);
        Assert.Equal(["DeliverAt", "ScheduleId"], indexColumns);
    }

    [Fact]
    public async Task ScheduleAsync_WithoutActiveTransaction_WritesStandaloneDelayedRecord()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var correlationId = Guid.NewGuid();
        var deliverAt = UtcNow.AddHours(2);
        var message = new OrderCreatedEvent(Guid.NewGuid());

        Assert.Null(unitOfWork.CurrentTransaction);

        var scheduleId = await scheduler.ScheduleAsync(
            message,
            deliverAt,
            new MessageDispatchOptions { CorrelationId = correlationId });

        Assert.Null(unitOfWork.CurrentTransaction);
        var row = Assert.Single(await GetDelayedRowsAsync());
        Assert.Equal(scheduleId, row.ScheduleId);
        Assert.NotEqual(Guid.Empty, row.MessageId);
        Assert.Contains(nameof(OrderCreatedEvent), row.ContractType);
        Assert.Contains(message.OrderId.ToString(), row.MessageJson);
        Assert.Equal(correlationId, row.CorrelationId);
        Assert.Equal(UtcNow, row.CreatedAt);
        Assert.Equal(deliverAt, row.DeliverAt);
    }

    [Fact]
    public async Task ScheduleAsync_WhenActiveTransactionCommits_PersistsDelayedRecord()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();

        await unitOfWork.BeginAsync();
        await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow.AddMinutes(10));
        await unitOfWork.CommitAsync();

        Assert.Equal(1, await CountDelayedMessagesAsync());
    }

    [Fact]
    public async Task ScheduleAsync_WhenActiveTransactionRollsBack_DoesNotPersistDelayedRecord()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();

        await unitOfWork.BeginAsync();
        await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow.AddMinutes(10));
        await unitOfWork.RollbackAsync();

        Assert.Equal(0, await CountDelayedMessagesAsync());
    }

    [Fact]
    public async Task CancelAsync_WithoutActiveTransaction_RemovesDelayedRecord()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var scheduleId = await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow.AddMinutes(10));

        var firstCancellation = await scheduler.CancelAsync(scheduleId);
        var secondCancellation = await scheduler.CancelAsync(scheduleId);

        Assert.True(firstCancellation);
        Assert.False(secondCancellation);
        Assert.Equal(0, await CountDelayedMessagesAsync());
    }

    [Fact]
    public async Task CancelAsync_ParticipatesInActiveTransaction()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var scheduleId = await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow.AddMinutes(10));

        await unitOfWork.BeginAsync();
        Assert.True(await scheduler.CancelAsync(scheduleId));
        await unitOfWork.RollbackAsync();

        Assert.Equal(1, await CountDelayedMessagesAsync());

        await unitOfWork.BeginAsync();
        Assert.True(await scheduler.CancelAsync(scheduleId));
        await unitOfWork.CommitAsync();

        Assert.Equal(0, await CountDelayedMessagesAsync());
    }

    [Fact]
    public async Task MoveDueToOutboxAsync_MovesPastAndBoundaryMessagesButKeepsFutureMessage()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var store = scope.ServiceProvider.GetRequiredService<IDelayedMessageStore>();
        var expiredScheduleId = await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow.AddTicks(-1));
        var boundaryScheduleId = await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow);
        var futureScheduleId = await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow.AddTicks(1));
        var scheduledRows = (await GetDelayedRowsAsync())
            .ToDictionary(row => row.ScheduleId);

        var movedCount = await store.MoveDueToOutboxAsync(UtcNow, batchSize: 10);

        Assert.Equal(2, movedCount);
        var remainingScheduleIds = (await GetDelayedRowsAsync())
            .Select(row => row.ScheduleId)
            .ToArray();
        Assert.DoesNotContain(expiredScheduleId, remainingScheduleIds);
        Assert.DoesNotContain(boundaryScheduleId, remainingScheduleIds);
        Assert.Contains(futureScheduleId, remainingScheduleIds);

        var outboxRows = await GetOutboxRowsAsync();
        Assert.Equal(2, outboxRows.Length);
        var expiredDelayedRow = scheduledRows[expiredScheduleId];
        var expiredOutboxRow = Assert.Single(
            outboxRows,
            row => row.MessageId == expiredDelayedRow.MessageId);
        Assert.Equal(expiredDelayedRow.ContractType, expiredOutboxRow.ContractType);
        Assert.Equal(expiredDelayedRow.MessageJson, expiredOutboxRow.MessageJson);
        Assert.Equal(expiredDelayedRow.CorrelationId, expiredOutboxRow.CorrelationId);
        Assert.Equal(expiredDelayedRow.CreatedAt, expiredOutboxRow.CreatedAt);
        Assert.Contains(
            outboxRows,
            row => row.MessageId == scheduledRows[boundaryScheduleId].MessageId);
        Assert.All(outboxRows, row => Assert.Equal(UtcNow, row.AvailableAt));
    }

    [Fact]
    public async Task MoveDueToOutboxAsync_WhenBatchIsLimited_MovesOldestMessagesFirst()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var store = scope.ServiceProvider.GetRequiredService<IDelayedMessageStore>();
        var oldestScheduleId = await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow.AddHours(-3));
        var middleScheduleId = await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow.AddHours(-2));
        var newestScheduleId = await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow.AddHours(-1));
        var scheduledRows = (await GetDelayedRowsAsync())
            .ToDictionary(row => row.ScheduleId);

        var movedCount = await store.MoveDueToOutboxAsync(UtcNow, batchSize: 2);

        Assert.Equal(2, movedCount);
        var outboxMessageIds = (await GetOutboxRowsAsync())
            .Select(row => row.MessageId)
            .ToArray();
        Assert.Contains(scheduledRows[oldestScheduleId].MessageId, outboxMessageIds);
        Assert.Contains(scheduledRows[middleScheduleId].MessageId, outboxMessageIds);
        Assert.DoesNotContain(scheduledRows[newestScheduleId].MessageId, outboxMessageIds);

        var remaining = Assert.Single(await GetDelayedRowsAsync());
        Assert.Equal(newestScheduleId, remaining.ScheduleId);
    }

    [Fact]
    public async Task MoveDueToOutboxAsync_WhenSeveralInstancesRunConcurrently_DoesNotMoveMessageTwice()
    {
        const int workerCount = 4;
        const int batchSize = 10;
        var rows = Enumerable.Range(0, workerCount * batchSize)
            .Select(index => CreateDelayedRow(UtcNow.AddMinutes(-(index + 1))))
            .ToArray();
        await InsertDelayedRowsAsync(rows);

        var tasks = Enumerable.Range(0, workerCount)
            .Select(_ => MoveBatchInNewScopeAsync(UtcNow, batchSize))
            .ToArray();
        var movedCounts = await Task.WhenAll(tasks);
        var totalMoved = movedCounts.Sum();

        while (true)
        {
            var movedCount = await MoveBatchInNewScopeAsync(UtcNow, batchSize);
            if (movedCount == 0)
            {
                break;
            }

            totalMoved += movedCount;
        }

        Assert.Equal(rows.Length, totalMoved);
        Assert.Equal(0, await CountDelayedMessagesAsync());
        Assert.Equal(rows.Length, await CountOutboxMessagesAsync());
    }

    [Fact]
    public async Task MoveDueToOutboxAsync_WhenOutboxInsertFails_RollsBackDelayedDelete()
    {
        var delayedRow = CreateDelayedRow(UtcNow.AddMinutes(-1));
        await InsertDelayedRowsAsync([delayedRow]);
        await InsertOutboxRowAsync(delayedRow.MessageId);
        await using var scope = _host!.Services.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IDelayedMessageStore>();

        await Assert.ThrowsAsync<SqlException>(() =>
            store.MoveDueToOutboxAsync(UtcNow, batchSize: 10));

        Assert.Equal(1, await CountDelayedMessagesAsync());
        Assert.Equal(1, await CountOutboxMessagesAsync());
    }

    [Fact]
    public async Task HostedWorker_MovesDueMessageToOutbox()
    {
        await using (var scope = _host!.Services.CreateAsyncScope())
        {
            var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
            await scheduler.ScheduleAsync(
                new OrderCreatedEvent(Guid.NewGuid()),
                UtcNow.AddMinutes(-1));
        }

        await _host!.StartAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        while (await CountOutboxMessagesAsync() == 0)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(25), timeout.Token);
        }

        await _host.StopAsync(timeout.Token);

        Assert.Equal(0, await CountDelayedMessagesAsync());
        Assert.Equal(1, await CountOutboxMessagesAsync());
    }

    public async Task InitializeAsync()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddSingleton<TimeProvider>(new FixedTimeProvider(UtcNow));
        builder.Services.AddScoped<IIntegrationUnitOfWork>(_ =>
            new IntegrationUnitOfWork(_connectionString));

        builder.Services
            .AddAsyncomniaMessaging()
            .UseSqlServer(options => options.ConnectionString = _connectionString)
            .UseAdoNet<IIntegrationUnitOfWork>()
            .UseDelayedPublishing();

        _host = builder.Build();

        await _host.MigrateAsyncomniaAsync();
        await CleanDatabaseAsync();
    }

    public Task DisposeAsync()
    {
        _host?.Dispose();
        return Task.CompletedTask;
    }

    private async Task<int> MoveBatchInNewScopeAsync(
        DateTimeOffset utcNow,
        int batchSize)
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IDelayedMessageStore>();

        return await store.MoveDueToOutboxAsync(utcNow, batchSize);
    }

    private async Task<DelayedRow[]> GetDelayedRowsAsync()
    {
        const string sql = """
            SELECT
                [ScheduleId],
                [MessageId],
                [ContractType],
                [MessageJson],
                [CorrelationId],
                [CreatedAt],
                [DeliverAt]
            FROM [asyncomnia].[Delayed]
            """;
        await using var connection = new SqlConnection(_connectionString);
        return (await connection.QueryAsync<DelayedRow>(sql)).ToArray();
    }

    private async Task<OutboxRow[]> GetOutboxRowsAsync()
    {
        const string sql = """
            SELECT
                [MessageId],
                [ContractType],
                [MessageJson],
                [CorrelationId],
                [CreatedAt],
                [AvailableAt]
            FROM [asyncomnia].[Outbox]
            """;
        await using var connection = new SqlConnection(_connectionString);
        return (await connection.QueryAsync<OutboxRow>(sql)).ToArray();
    }

    private async Task InsertDelayedRowsAsync(IEnumerable<DelayedRow> rows)
    {
        const string sql = """
            INSERT INTO [asyncomnia].[Delayed]
            (
                [ScheduleId],
                [MessageId],
                [ContractType],
                [MessageJson],
                [CorrelationId],
                [CreatedAt],
                [DeliverAt]
            )
            VALUES
            (
                @ScheduleId,
                @MessageId,
                @ContractType,
                @MessageJson,
                @CorrelationId,
                @CreatedAt,
                @DeliverAt
            )
            """;
        await using var connection = new SqlConnection(_connectionString);
        await connection.ExecuteAsync(sql, rows);
    }

    private async Task InsertOutboxRowAsync(Guid messageId)
    {
        const string sql = """
            INSERT INTO [asyncomnia].[Outbox]
            (
                [MessageId],
                [ContractType],
                [MessageJson],
                [CreatedAt],
                [AvailableAt]
            )
            VALUES
            (
                @MessageId,
                N'Tests.OrderCreatedEvent, Tests',
                N'{}',
                @CreatedAt,
                @AvailableAt
            )
            """;
        await using var connection = new SqlConnection(_connectionString);
        await connection.ExecuteAsync(sql, new
        {
            MessageId = messageId,
            CreatedAt = UtcNow,
            AvailableAt = UtcNow
        });
    }

    private async Task CleanDatabaseAsync()
    {
        await ExecuteNonQueryAsync("""
            DELETE FROM [asyncomnia].[Delayed];
            DELETE FROM [asyncomnia].[Outbox];
            """);
    }

    private Task<int> CountDelayedMessagesAsync()
    {
        return ExecuteScalarAsync<int>(
            "SELECT COUNT(*) FROM [asyncomnia].[Delayed]");
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

    private static DelayedRow CreateDelayedRow(DateTimeOffset deliverAt)
    {
        return new DelayedRow
        {
            ScheduleId = Guid.NewGuid(),
            MessageId = Guid.NewGuid(),
            ContractType = "Tests.OrderCreatedEvent, Tests",
            MessageJson = "{}",
            CorrelationId = Guid.NewGuid(),
            CreatedAt = UtcNow,
            DeliverAt = deliverAt
        };
    }

    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => utcNow;
    }

    private sealed class DelayedRow
    {
        public Guid ScheduleId { get; init; }
        public Guid MessageId { get; init; }
        public string ContractType { get; init; } = null!;
        public string MessageJson { get; init; } = null!;
        public Guid? CorrelationId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public DateTimeOffset DeliverAt { get; init; }
    }

    private sealed class OutboxRow
    {
        public Guid MessageId { get; init; }
        public string ContractType { get; init; } = null!;
        public string MessageJson { get; init; } = null!;
        public Guid? CorrelationId { get; init; }
        public DateTimeOffset CreatedAt { get; init; }
        public DateTimeOffset AvailableAt { get; init; }
    }
}
