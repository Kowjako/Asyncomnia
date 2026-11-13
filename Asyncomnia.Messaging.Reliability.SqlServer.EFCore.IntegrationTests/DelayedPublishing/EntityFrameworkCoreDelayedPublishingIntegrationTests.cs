using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.Infrastructure;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.TestDoubles;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.DelayedPublishing;

[Collection(SqlServerCollection.Name)]
public sealed class EntityFrameworkCoreDelayedPublishingIntegrationTests : IAsyncLifetime
{
    private static readonly DateTimeOffset UtcNow =
        new(2026, 11, 15, 10, 0, 0, TimeSpan.Zero);

    private readonly string _connectionString;
    private IHost? _host;

    public EntityFrameworkCoreDelayedPublishingIntegrationTests(
        SqlServerContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task ScheduleAsync_DoesNotWriteDelayedRecordUntilSaveChanges()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<IntegrationDbContext>();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();

        await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow.AddMinutes(15));

        Assert.Equal(0, await CountDelayedMessagesAsync());

        await context.SaveChangesAsync();

        Assert.Equal(1, await CountDelayedMessagesAsync());
    }

    [Fact]
    public async Task SaveChangesAndCommit_WritesDomainAndDelayedRecordsTogether()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<IntegrationDbContext>();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var orderId = Guid.NewGuid();

        await using (var transaction = await context.Database.BeginTransactionAsync())
        {
            context.Orders.Add(new TestOrder(orderId, "Scheduled order"));
            await scheduler.ScheduleAsync(
                new OrderCreatedEvent(orderId),
                UtcNow.AddMinutes(15));

            await context.SaveChangesAsync();
            await transaction.CommitAsync();
        }

        Assert.Equal(1, await context.Orders.CountAsync());
        Assert.Equal(1, await CountDelayedMessagesAsync());
    }

    [Fact]
    public async Task SaveChangesAndRollback_DoesNotWriteDomainOrDelayedRecords()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<IntegrationDbContext>();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var orderId = Guid.NewGuid();

        await using (var transaction = await context.Database.BeginTransactionAsync())
        {
            context.Orders.Add(new TestOrder(orderId, "Rolled back scheduled order"));
            await scheduler.ScheduleAsync(
                new OrderCreatedEvent(orderId),
                UtcNow.AddMinutes(15));

            await context.SaveChangesAsync();
            await transaction.RollbackAsync();
        }

        context.ChangeTracker.Clear();

        Assert.Equal(0, await context.Orders.CountAsync());
        Assert.Equal(0, await CountDelayedMessagesAsync());
    }

    [Fact]
    public async Task CancelAsync_WhenEntryIsPending_PreventsInsertDuringSaveChanges()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<IntegrationDbContext>();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var scheduleId = await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow.AddMinutes(15));

        var cancelled = await scheduler.CancelAsync(scheduleId);
        await context.SaveChangesAsync();

        Assert.True(cancelled);
        Assert.Equal(0, await CountDelayedMessagesAsync());
    }

    [Fact]
    public async Task CancelAsync_WhenEntryWasSaved_DeletesPersistedRecord()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<IntegrationDbContext>();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var scheduleId = await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow.AddMinutes(15));
        await context.SaveChangesAsync();

        var firstCancellation = await scheduler.CancelAsync(scheduleId);
        var secondCancellation = await scheduler.CancelAsync(scheduleId);

        Assert.True(firstCancellation);
        Assert.False(secondCancellation);
        Assert.Equal(0, await CountDelayedMessagesAsync());
    }

    [Fact]
    public async Task CancelAsync_WhenTransactionRollsBack_RestoresPersistedRecord()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<IntegrationDbContext>();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var scheduleId = await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow.AddMinutes(15));
        await context.SaveChangesAsync();

        await using (var transaction = await context.Database.BeginTransactionAsync())
        {
            Assert.True(await scheduler.CancelAsync(scheduleId));
            await transaction.RollbackAsync();
        }

        Assert.Equal(1, await CountDelayedMessagesAsync());
    }

    [Fact]
    public async Task MoveDueToOutboxAsync_MovesMessageWrittenByEntityFrameworkCore()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<IntegrationDbContext>();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var store = scope.ServiceProvider.GetRequiredService<IDelayedMessageStore>();
        await scheduler.ScheduleAsync(
            new OrderCreatedEvent(Guid.NewGuid()),
            UtcNow.AddMinutes(-1));
        await context.SaveChangesAsync();

        var movedCount = await store.MoveDueToOutboxAsync(UtcNow, batchSize: 10);

        Assert.Equal(1, movedCount);
        Assert.Equal(0, await CountDelayedMessagesAsync());
        Assert.Equal(1, await CountOutboxMessagesAsync());
    }

    public async Task InitializeAsync()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddSingleton<TimeProvider>(new FixedTimeProvider(UtcNow));
        builder.Services.AddDbContext<IntegrationDbContext>(options =>
            options.UseSqlServer(_connectionString));

        builder.Services
            .AddAsyncomniaMessaging()
            .UseSqlServer(options => options.ConnectionString = _connectionString)
            .UseEntityFrameworkCore<IntegrationDbContext>()
            .UseDelayedPublishing();

        _host = builder.Build();

        await _host.MigrateAsyncomniaAsync();
        await EnsureDomainSchemaAsync();
        await CleanDatabaseAsync();
    }

    public Task DisposeAsync()
    {
        _host?.Dispose();
        return Task.CompletedTask;
    }

    private async Task EnsureDomainSchemaAsync()
    {
        await ExecuteNonQueryAsync("""
            IF OBJECT_ID(N'[dbo].[Orders]', N'U') IS NULL
            BEGIN
                CREATE TABLE [dbo].[Orders]
                (
                    [Id] uniqueidentifier NOT NULL,
                    [Description] nvarchar(200) NOT NULL,
                    CONSTRAINT [PK_Orders] PRIMARY KEY CLUSTERED ([Id])
                );
            END;
            """);
    }

    private async Task CleanDatabaseAsync()
    {
        await ExecuteNonQueryAsync("""
            DELETE FROM [asyncomnia].[Delayed];
            DELETE FROM [asyncomnia].[Outbox];
            DELETE FROM [dbo].[Orders];
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

    private async Task<T> ExecuteScalarAsync<T>(string sql)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        var result = await command.ExecuteScalarAsync();
        return (T)Convert.ChangeType(result!, typeof(T));
    }

    private async Task ExecuteNonQueryAsync(string sql)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        await command.ExecuteNonQueryAsync();
    }

    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => utcNow;
    }
}
