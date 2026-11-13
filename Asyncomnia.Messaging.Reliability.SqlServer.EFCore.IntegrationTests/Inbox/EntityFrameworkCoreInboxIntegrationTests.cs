using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.Infrastructure;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.TestDoubles;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.Inbox;

[Collection(SqlServerCollection.Name)]
public sealed class EntityFrameworkCoreInboxIntegrationTests : IAsyncLifetime
{
    private const string ConsumerId =
        "events/orders/subscription/billing-service";

    private readonly string _connectionString;
    private readonly InboxHandlerProbe _handlerProbe = new();
    private IHost? _host;

    public EntityFrameworkCoreInboxIntegrationTests(SqlServerContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task HandleEventAsync_WhenMessageIsNew_CommitsInboxAndTrackedDomainChanges()
    {
        await DispatchAsync(
            Guid.NewGuid(),
            new InboxOrderCreatedEvent(Guid.NewGuid(), "EF Core delivery"));

        Assert.Equal(1, _handlerProbe.Attempts);
        Assert.Equal(1, await CountInboxMessagesAsync());
        Assert.Equal(1, await CountOrdersAsync());
        Assert.Equal(0, await CountOutboxMessagesAsync());
    }

    [Fact]
    public async Task HandleEventAsync_WhenMessageIsDeliveredTwice_HandlesItOnce()
    {
        var messageId = Guid.NewGuid();
        var message = new InboxOrderCreatedEvent(Guid.NewGuid(), "Duplicate EF delivery");

        await DispatchAsync(messageId, message);
        await DispatchAsync(messageId, message);

        Assert.Equal(1, _handlerProbe.Attempts);
        Assert.Equal(1, await CountInboxMessagesAsync());
        Assert.Equal(1, await CountOrdersAsync());
    }

    [Fact]
    public async Task HandleEventAsync_WhenHandlerPublishesEvent_CommitsInboxDomainAndOutbox()
    {
        _handlerProbe.PublishOutboxMessage = true;
        var orderId = Guid.NewGuid();

        await DispatchAsync(
            Guid.NewGuid(),
            new InboxOrderCreatedEvent(orderId, "EF delivery with Outbox"));

        Assert.Equal(1, _handlerProbe.Attempts);
        Assert.Equal(1, await CountInboxMessagesAsync());
        Assert.Equal(1, await CountOrdersAsync());
        Assert.Equal(1, await CountOutboxMessagesAsync());

        var outboxJson = await ExecuteScalarAsync<string>(
            "SELECT TOP (1) [MessageJson] FROM [asyncomnia].[Outbox]");
        Assert.Contains(orderId.ToString(), outboxJson);
    }

    [Fact]
    public async Task HandleEventAsync_WhenHandlerFails_RollsBackInboxDomainAndOutbox()
    {
        _handlerProbe.PublishOutboxMessage = true;
        _handlerProbe.ThrowAfterChanges = true;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            DispatchAsync(
                Guid.NewGuid(),
                new InboxOrderCreatedEvent(Guid.NewGuid(), "Failed EF delivery")));

        Assert.Equal(1, _handlerProbe.Attempts);
        Assert.Equal(0, await CountInboxMessagesAsync());
        Assert.Equal(0, await CountOrdersAsync());
        Assert.Equal(0, await CountOutboxMessagesAsync());
    }

    public async Task InitializeAsync()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddSingleton(_handlerProbe);
        builder.Services.AddDbContext<IntegrationDbContext>(options =>
            options.UseSqlServer(_connectionString));

        builder.Services
            .AddAsyncomniaMessaging()
            .RegisterEventHandler<InboxOrderCreatedEvent, InboxOrderCreatedEventHandler>()
            .UseSqlServer(options =>
                options.ConnectionString = _connectionString)
            .UseEntityFrameworkCore<IntegrationDbContext>()
            .UseOutbox()
            .UseInbox();

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

    private async Task DispatchAsync(Guid messageId, InboxOrderCreatedEvent message)
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<IInboundMessagePipeline>();
        var context = new InboundMessageContext(
            ConsumerId,
            $"{typeof(InboxOrderCreatedEvent).FullName}, {typeof(InboxOrderCreatedEvent).Assembly.GetName().Name}",
            message,
            new MessageMetadata(messageId, DateTimeOffset.UtcNow));

        await pipeline.HandleEventAsync<InboxOrderCreatedEvent>(context);
    }

    private Task<int> CountInboxMessagesAsync()
    {
        return ExecuteScalarAsync<int>("SELECT COUNT(*) FROM [asyncomnia].[Inbox]");
    }

    private Task<int> CountOrdersAsync()
    {
        return ExecuteScalarAsync<int>("SELECT COUNT(*) FROM [dbo].[Orders]");
    }

    private Task<int> CountOutboxMessagesAsync()
    {
        return ExecuteScalarAsync<int>("SELECT COUNT(*) FROM [asyncomnia].[Outbox]");
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
            DELETE FROM [asyncomnia].[Inbox];
            DELETE FROM [asyncomnia].[Outbox];
            DELETE FROM [dbo].[Orders];
            """;

        await ExecuteNonQueryAsync(sql);
    }

    private async Task<T?> ExecuteScalarAsync<T>(string sql)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        var result = await command.ExecuteScalarAsync();
        return result is null or DBNull ? default : (T)result;
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
