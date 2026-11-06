using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.IntegrationTests.Infrastructure;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.IntegrationTests.TestDoubles;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.IntegrationTests.Outbox;

[Collection(SqlServerCollection.Name)]
public sealed class DapperOutboxIntegrationTests : IAsyncLifetime
{
    private readonly string _connectionString;
    private IHost? _host;

    public DapperOutboxIntegrationTests(SqlServerContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task Commit_WritesDomainAndOutboxRecords()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();
        var orderId = Guid.NewGuid();

        await unitOfWork.BeginAsync();
        await unitOfWork.InsertOrderAsync(orderId, "Committed order");
        await publisher.PublishAsync(new OrderCreatedEvent(orderId));
        await unitOfWork.CommitAsync();

        Assert.Equal(1, await CountOrdersAsync());
        Assert.Equal(1, await CountOutboxMessagesAsync());

        var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
        var claimedMessages = await store.ClaimAsync(10, TimeSpan.FromMinutes(1));
        var claimedMessage = Assert.Single(claimedMessages);

        Assert.Contains(nameof(OrderCreatedEvent), claimedMessage.Message.ContractType);
        Assert.Contains(orderId.ToString(), claimedMessage.Message.MessageJson);
    }

    [Fact]
    public async Task Rollback_DoesNotWriteDomainOrOutboxRecords()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();
        var orderId = Guid.NewGuid();

        await unitOfWork.BeginAsync();
        await unitOfWork.InsertOrderAsync(orderId, "Rolled back order");
        await publisher.PublishAsync(new OrderCreatedEvent(orderId));
        await unitOfWork.RollbackAsync();

        Assert.Equal(0, await CountOrdersAsync());
        Assert.Equal(0, await CountOutboxMessagesAsync());
    }

    public async Task InitializeAsync()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddScoped<IIntegrationUnitOfWork>(_ =>
            new IntegrationUnitOfWork(_connectionString));

        builder.Services
            .AddAsyncomniaMessaging()
            .UseOutbox(outbox =>
            {
                outbox.UseSqlServer(options =>
                    options.ConnectionString = _connectionString);
                outbox.UseDapper<IIntegrationUnitOfWork>();
            });

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
        const string sql = @"
            IF OBJECT_ID(N'[dbo].[Orders]', N'U') IS NULL
            BEGIN
                CREATE TABLE [dbo].[Orders]
                (
                    [Id] uniqueidentifier NOT NULL,
                    [Description] nvarchar(200) NOT NULL,
                    CONSTRAINT [PK_Orders] PRIMARY KEY CLUSTERED ([Id])
                );
            END";

        await ExecuteNonQueryAsync(sql);
    }

    private async Task CleanDatabaseAsync()
    {
        const string sql = @"
            DELETE FROM [asyncomnia].[Outbox];
            DELETE FROM [dbo].[Orders]";

        await ExecuteNonQueryAsync(sql);
    }

    private Task<int> CountOrdersAsync()
    {
        return ExecuteScalarAsync<int>("SELECT COUNT(*) FROM [dbo].[Orders]");
    }

    private Task<int> CountOutboxMessagesAsync()
    {
        return ExecuteScalarAsync<int>("SELECT COUNT(*) FROM [asyncomnia].[Outbox]");
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
}
