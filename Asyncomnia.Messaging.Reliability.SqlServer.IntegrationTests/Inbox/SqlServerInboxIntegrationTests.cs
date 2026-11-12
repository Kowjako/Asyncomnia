using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.Infrastructure;
using Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.TestDoubles;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.Inbox;

[Collection(SqlServerCollection.Name)]
public sealed class SqlServerInboxIntegrationTests : IAsyncLifetime
{
    private const string DefaultConsumerId =
        "events/orders/subscription/billing-service";

    private readonly string _connectionString;
    private readonly InboxHandlerProbe _handlerProbe = new();
    private IHost? _host;

    public SqlServerInboxIntegrationTests(SqlServerContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task Migration_CreatesInboxTable()
    {
        var objectId = await ExecuteScalarAsync<int?>(
            "SELECT OBJECT_ID(N'[asyncomnia].[Inbox]', N'U')");

        Assert.NotNull(objectId);
    }

    [Fact]
    public async Task HandleEventAsync_WhenMessageIsNew_CommitsInboxAndHandlerChanges()
    {
        var messageId = Guid.NewGuid();
        var orderId = Guid.NewGuid();

        await DispatchAsync(
            messageId,
            DefaultConsumerId,
            new InboxOrderCreatedEvent(orderId, "First delivery"));

        Assert.Equal(1, _handlerProbe.Attempts);
        Assert.Equal(1, await CountInboxMessagesAsync());
        Assert.Equal(1, await CountOrdersAsync());
        Assert.Equal(0, await CountOutboxMessagesAsync());
    }

    [Fact]
    public async Task HandleEventAsync_WhenMessageIsDeliveredTwiceToSameConsumer_HandlesItOnce()
    {
        var messageId = Guid.NewGuid();
        var message = new InboxOrderCreatedEvent(Guid.NewGuid(), "Duplicate delivery");

        await DispatchAsync(messageId, DefaultConsumerId, message);
        await DispatchAsync(messageId, DefaultConsumerId, message);

        Assert.Equal(1, _handlerProbe.Attempts);
        Assert.Equal(1, await CountInboxMessagesAsync());
        Assert.Equal(1, await CountOrdersAsync());
    }

    [Fact]
    public async Task HandleEventAsync_WhenDuplicateDeliveriesRace_HandlesMessageOnce()
    {
        var messageId = Guid.NewGuid();
        var message = new InboxOrderCreatedEvent(Guid.NewGuid(), "Concurrent duplicate");

        await Task.WhenAll(
            DispatchAsync(messageId, DefaultConsumerId, message),
            DispatchAsync(messageId, DefaultConsumerId, message));

        Assert.Equal(1, _handlerProbe.Attempts);
        Assert.Equal(1, await CountInboxMessagesAsync());
        Assert.Equal(1, await CountOrdersAsync());
    }

    [Fact]
    public async Task HandleEventAsync_WhenMessageReachesDifferentConsumers_HandlesItForEachConsumer()
    {
        var messageId = Guid.NewGuid();

        await DispatchAsync(
            messageId,
            "events/orders/subscription/billing-service",
            new InboxOrderCreatedEvent(Guid.NewGuid(), "Billing delivery"));
        await DispatchAsync(
            messageId,
            "events/orders/subscription/auditing-service",
            new InboxOrderCreatedEvent(Guid.NewGuid(), "Auditing delivery"));

        Assert.Equal(2, _handlerProbe.Attempts);
        Assert.Equal(2, await CountInboxMessagesAsync());
        Assert.Equal(2, await CountOrdersAsync());
    }

    [Fact]
    public async Task HandleEventAsync_WhenHandlerFails_RollsBackInboxAndHandlerChanges()
    {
        _handlerProbe.ThrowAfterInsert = true;

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            DispatchAsync(
                Guid.NewGuid(),
                DefaultConsumerId,
                new InboxOrderCreatedEvent(Guid.NewGuid(), "Failed delivery")));

        Assert.Equal("Simulated handler failure.", exception.Message);
        Assert.Equal(1, _handlerProbe.Attempts);
        Assert.Equal(0, await CountInboxMessagesAsync());
        Assert.Equal(0, await CountOrdersAsync());
        Assert.Equal(0, await CountOutboxMessagesAsync());
    }

    [Fact]
    public async Task HandleEventAsync_WhenHandlerFailsAfterPublishing_RollsBackInboxDomainAndOutbox()
    {
        _handlerProbe.PublishOutboxMessage = true;
        _handlerProbe.ThrowAfterInsert = true;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            DispatchAsync(
                Guid.NewGuid(),
                DefaultConsumerId,
                new InboxOrderCreatedEvent(Guid.NewGuid(), "Failed after publish")));

        Assert.Equal(1, _handlerProbe.Attempts);
        Assert.Equal(0, await CountInboxMessagesAsync());
        Assert.Equal(0, await CountOrdersAsync());
        Assert.Equal(0, await CountOutboxMessagesAsync());
    }

    [Fact]
    public async Task HandleEventAsync_WhenHandlerPublishesEvent_CommitsInboxDomainAndOutboxTogether()
    {
        _handlerProbe.PublishOutboxMessage = true;
        var orderId = Guid.NewGuid();

        await DispatchAsync(
            Guid.NewGuid(),
            DefaultConsumerId,
            new InboxOrderCreatedEvent(orderId, "Delivery with outgoing event"));

        Assert.Equal(1, _handlerProbe.Attempts);
        Assert.Equal(1, await CountInboxMessagesAsync());
        Assert.Equal(1, await CountOrdersAsync());
        Assert.Equal(1, await CountOutboxMessagesAsync());

        var outboxJson = await ExecuteScalarAsync<string>(
            "SELECT TOP (1) [MessageJson] FROM [asyncomnia].[Outbox]");
        Assert.Contains(orderId.ToString(), outboxJson);
    }

    public async Task InitializeAsync()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddSingleton(_handlerProbe);
        builder.Services.AddScoped<IIntegrationUnitOfWork>(_ =>
            new IntegrationUnitOfWork(_connectionString));

        builder.Services
            .AddAsyncomniaMessaging()
            .RegisterEventHandler<InboxOrderCreatedEvent, InboxOrderCreatedEventHandler>()
            .UseOutbox(outbox =>
                outbox.UseSqlServer<IIntegrationUnitOfWork>(options =>
                    options.ConnectionString = _connectionString))
            .UseInbox(inbox =>
                inbox.UseSqlServer<IIntegrationUnitOfWork>());

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

    private async Task DispatchAsync(
        Guid messageId,
        string consumerId,
        InboxOrderCreatedEvent message)
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<IInboundMessagePipeline>();
        var context = new InboundMessageContext(
            consumerId,
            $"{typeof(InboxOrderCreatedEvent).FullName}, {typeof(InboxOrderCreatedEvent).Assembly.GetName().Name}",
            message,
            new MessageMetadata(messageId, DateTimeOffset.UtcNow));

        await pipeline.HandleEventAsync<InboxOrderCreatedEvent>(context);
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
            DELETE FROM [asyncomnia].[Inbox];
            DELETE FROM [asyncomnia].[Outbox];
            DELETE FROM [dbo].[Orders]";

        await ExecuteNonQueryAsync(sql);
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
