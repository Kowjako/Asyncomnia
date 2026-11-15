using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.Infrastructure;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.TestDoubles;
using Asyncomnia.Messaging.Sagas.Persistence;
using Asyncomnia.Messaging.Sagas.StateMachine;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.Sagas;

[Collection(SqlServerCollection.Name)]
public sealed class EntityFrameworkCoreSagaRepositoryIntegrationTests : IAsyncLifetime
{
    private readonly string _connectionString;
    private IHost? _host;

    public EntityFrameworkCoreSagaRepositoryIntegrationTests(
        SqlServerContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task CommitAsync_PersistsSagaAndTrackedDomainChanges()
    {
        var saga = CreateSaga();
        var orderId = Guid.NewGuid();

        await using (var scope = _host!.Services.CreateAsyncScope())
        {
            var transaction = scope.ServiceProvider
                .GetRequiredService<IInboundTransaction>();
            var repository = scope.ServiceProvider
                .GetRequiredService<ISagaRepository<EntityFrameworkCoreOrderSaga>>();
            var context = scope.ServiceProvider
                .GetRequiredService<IntegrationDbContext>();

            await transaction.BeginAsync();
            context.Orders.Add(new TestOrder(orderId, "Committed with saga"));
            Assert.True(await repository.TryInsertAsync(saga, default));
            await transaction.CommitAsync();
        }

        var persisted = Assert.IsType<EntityFrameworkCoreOrderSaga>(
            await LoadSagaAsync(saga.CorrelationId));

        Assert.Equal(saga.CorrelationId, persisted.CorrelationId);
        Assert.Equal(saga.CurrentState, persisted.CurrentState);
        Assert.Equal(saga.OrderNumber, persisted.OrderNumber);
        Assert.Equal(1, await CountAsync("[asyncomnia].[Saga]"));
        Assert.Equal(1, await CountAsync("[dbo].[Orders]"));
    }

    [Fact]
    public async Task RollbackAsync_RollsBackSagaAndSavedDomainChanges()
    {
        var saga = CreateSaga();

        await using (var scope = _host!.Services.CreateAsyncScope())
        {
            var transaction = scope.ServiceProvider
                .GetRequiredService<IInboundTransaction>();
            var repository = scope.ServiceProvider
                .GetRequiredService<ISagaRepository<EntityFrameworkCoreOrderSaga>>();
            var context = scope.ServiceProvider
                .GetRequiredService<IntegrationDbContext>();

            await transaction.BeginAsync();
            context.Orders.Add(new TestOrder(Guid.NewGuid(), "Rolled back with saga"));
            await context.SaveChangesAsync();
            Assert.True(await repository.TryInsertAsync(saga, default));
            await transaction.RollbackAsync();
        }

        Assert.Equal(0, await CountAsync("[asyncomnia].[Saga]"));
        Assert.Equal(0, await CountAsync("[dbo].[Orders]"));
    }

    [Fact]
    public async Task CommitAsync_PersistsSagaUpdateAndTrackedDomainChanges()
    {
        var saga = CreateSaga();
        await InsertSagaAsync(saga);
        var orderId = Guid.NewGuid();

        await using (var scope = _host!.Services.CreateAsyncScope())
        {
            var transaction = scope.ServiceProvider
                .GetRequiredService<IInboundTransaction>();
            var repository = scope.ServiceProvider
                .GetRequiredService<ISagaRepository<EntityFrameworkCoreOrderSaga>>();
            var context = scope.ServiceProvider
                .GetRequiredService<IntegrationDbContext>();

            await transaction.BeginAsync();
            var loaded = Assert.IsType<EntityFrameworkCoreOrderSaga>(
                await repository.GetForUpdateAsync(saga.CorrelationId, default));
            loaded.CurrentState = "Completed";
            loaded.OrderNumber = "ORD-EF-UPDATED";
            context.Orders.Add(new TestOrder(orderId, "Committed with saga update"));

            await repository.UpdateAsync(loaded, default);
            await transaction.CommitAsync();
        }

        var persisted = Assert.IsType<EntityFrameworkCoreOrderSaga>(
            await LoadSagaAsync(saga.CorrelationId));

        Assert.Equal("Completed", persisted.CurrentState);
        Assert.Equal("ORD-EF-UPDATED", persisted.OrderNumber);
        Assert.Equal(1, await CountAsync("[asyncomnia].[Saga]"));
        Assert.Equal(1, await CountAsync("[dbo].[Orders]"));
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
            .UseEntityFrameworkCore<IntegrationDbContext>();

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

    private async Task InsertSagaAsync(EntityFrameworkCoreOrderSaga saga)
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var transaction = scope.ServiceProvider
            .GetRequiredService<IInboundTransaction>();
        var repository = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<EntityFrameworkCoreOrderSaga>>();

        await transaction.BeginAsync();
        Assert.True(await repository.TryInsertAsync(saga, default));
        await transaction.CommitAsync();
    }

    private async Task<EntityFrameworkCoreOrderSaga?> LoadSagaAsync(
        Guid correlationId)
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var transaction = scope.ServiceProvider
            .GetRequiredService<IInboundTransaction>();
        var repository = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<EntityFrameworkCoreOrderSaga>>();

        await transaction.BeginAsync();
        var saga = await repository.GetForUpdateAsync(correlationId, default);
        await transaction.CommitAsync();

        return saga;
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
            DELETE FROM [asyncomnia].[Saga];
            DELETE FROM [dbo].[Orders];
            """;

        await ExecuteNonQueryAsync(sql);
    }

    private async Task<int> CountAsync(string tableName)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {tableName}";

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

    private static EntityFrameworkCoreOrderSaga CreateSaga()
    {
        return new EntityFrameworkCoreOrderSaga
        {
            CorrelationId = Guid.NewGuid(),
            CurrentState = "Submitted",
            OrderNumber = "ORD-EF-001"
        };
    }
}

public sealed class EntityFrameworkCoreOrderSaga : AsyncomniaSagaInstance
{
    public string OrderNumber { get; set; } = null!;
}
