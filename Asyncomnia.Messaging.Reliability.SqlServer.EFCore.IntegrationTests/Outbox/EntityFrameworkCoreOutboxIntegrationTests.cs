using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.Infrastructure;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.TestDoubles;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.Outbox;

[Collection(SqlServerCollection.Name)]
public sealed class EntityFrameworkCoreOutboxIntegrationTests : IAsyncLifetime
{
    private readonly string _connectionString;
    private IHost? _host;

    public EntityFrameworkCoreOutboxIntegrationTests(SqlServerContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task SaveChangesAndCommit_WritesDomainAndOutboxRecords()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<IntegrationDbContext>();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();
        var orderId = Guid.NewGuid();

        await using (var transaction = await context.Database.BeginTransactionAsync())
        {
            context.Orders.Add(new TestOrder(orderId, "Committed order"));
            await publisher.PublishAsync(new OrderCreatedEvent(orderId));

            await context.SaveChangesAsync();
            await transaction.CommitAsync();
        }

        Assert.Equal(1, await context.Orders.CountAsync());
        Assert.Equal(1, await CountOutboxMessagesAsync());

        var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
        var claimedMessages = await store.ClaimAsync(10, TimeSpan.FromMinutes(1));
        var claimedMessage = Assert.Single(claimedMessages);

        Assert.Contains(nameof(OrderCreatedEvent), claimedMessage.Message.ContractType);
        Assert.Contains(orderId.ToString(), claimedMessage.Message.MessageJson);
    }

    [Fact]
    public async Task SaveChangesAndRollback_DoesNotWriteDomainOrOutboxRecords()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<IntegrationDbContext>();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();
        var orderId = Guid.NewGuid();

        await using (var transaction = await context.Database.BeginTransactionAsync())
        {
            context.Orders.Add(new TestOrder(orderId, "Rolled back order"));
            await publisher.PublishAsync(new OrderCreatedEvent(orderId));

            await context.SaveChangesAsync();
            await transaction.RollbackAsync();
        }

        context.ChangeTracker.Clear();

        Assert.Equal(0, await context.Orders.CountAsync());
        Assert.Equal(0, await CountOutboxMessagesAsync());
    }

    public async Task InitializeAsync()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddDbContext<IntegrationDbContext>(options =>
            options.UseSqlServer(_connectionString));

        builder.Services
            .AddAsyncomniaMessaging()
            .UseSqlServer(options =>
                options.ConnectionString = _connectionString)
            .UseEntityFrameworkCore<IntegrationDbContext>()
            .UseOutbox();

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
        const string sql = """
            IF OBJECT_ID(N'[dbo].[Orders]', N'U') IS NULL
            BEGIN
                CREATE TABLE [dbo].[Orders]
                (
                    [Id] uniqueidentifier NOT NULL,
                    [Description] nvarchar(200) NOT NULL,
                    CONSTRAINT [PK_Orders] PRIMARY KEY CLUSTERED ([Id])
                );
            END;
            """;

        await ExecuteNonQueryAsync(sql);
    }

    private async Task CleanDatabaseAsync()
    {
        const string sql = """
            DELETE FROM [asyncomnia].[Outbox];
            DELETE FROM [dbo].[Orders];
            """;

        await ExecuteNonQueryAsync(sql);
    }

    private async Task<int> CountOutboxMessagesAsync()
    {
        const string sql = "SELECT COUNT(*) FROM [asyncomnia].[Outbox]";

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    private async Task ExecuteNonQueryAsync(string sql)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        await command.ExecuteNonQueryAsync();
    }
}
