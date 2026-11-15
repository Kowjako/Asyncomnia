using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.Infrastructure;
using Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.TestDoubles;
using Asyncomnia.Messaging.Sagas.Persistence;
using Asyncomnia.Messaging.Sagas.StateMachine;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.Sagas;

[Collection(SqlServerCollection.Name)]
public sealed class SqlServerSagaRepositoryIntegrationTests : IAsyncLifetime
{
    private readonly string _connectionString;
    private IHost? _host;

    public SqlServerSagaRepositoryIntegrationTests(
        SqlServerContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task Migration_CreatesSagaTableWithCompositePrimaryKey()
    {
        var objectId = await ExecuteScalarAsync<int?>(
            "SELECT OBJECT_ID(N'[asyncomnia].[Saga]', N'U')");

        const string primaryKeySql = """
            SELECT column_definition.[name]
            FROM sys.indexes AS index_definition
            INNER JOIN sys.index_columns AS index_column
                ON index_column.[object_id] = index_definition.[object_id]
               AND index_column.[index_id] = index_definition.[index_id]
            INNER JOIN sys.columns AS column_definition
                ON column_definition.[object_id] = index_column.[object_id]
               AND column_definition.[column_id] = index_column.[column_id]
            WHERE index_definition.[is_primary_key] = 1
              AND index_definition.[object_id] = OBJECT_ID(N'[asyncomnia].[Saga]')
            ORDER BY index_column.[key_ordinal]
            """;
        await using var connection = new SqlConnection(_connectionString);
        var primaryKeyColumns = (await connection.QueryAsync<string>(primaryKeySql)).ToArray();

        Assert.NotNull(objectId);
        Assert.Equal(["SagaType", "CorrelationId"], primaryKeyColumns);
    }

    [Fact]
    public async Task TryInsertAsync_WhenTransactionCommits_PersistsAndHydratesSaga()
    {
        var instance = CreateOrderSaga();

        Assert.True(await InsertAndCommitAsync(instance));

        var loaded = Assert.IsType<IntegrationOrderSaga>(
            await LoadAsync<IntegrationOrderSaga>(instance.CorrelationId));
        Assert.NotSame(instance, loaded);
        Assert.Equal(instance.CorrelationId, loaded.CorrelationId);
        Assert.Equal(instance.CurrentState, loaded.CurrentState);
        Assert.Equal(instance.OrderNumber, loaded.OrderNumber);
        Assert.Equal(instance.AttemptNumber, loaded.AttemptNumber);

        var row = await GetSagaRowAsync(instance.CorrelationId);
        Assert.Contains(typeof(IntegrationOrderSaga).FullName!, row.SagaType);
        Assert.Contains(typeof(IntegrationOrderSaga).Assembly.GetName().Name!, row.SagaType);
        Assert.Contains(instance.OrderNumber, row.DataJson);
        Assert.True(row.UpdatedAt >= row.CreatedAt);
    }

    [Fact]
    public async Task TryInsertAsync_WhenTransactionRollsBack_DoesNotPersistSaga()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var repository = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<IntegrationOrderSaga>>();

        await unitOfWork.BeginAsync();
        Assert.True(await repository.TryInsertAsync(CreateOrderSaga(), default));
        await unitOfWork.RollbackAsync();

        Assert.Equal(0, await CountSagaRowsAsync());
    }

    [Fact]
    public async Task UpdateAsync_WhenTransactionCommits_PersistsStateAndDataChanges()
    {
        var instance = CreateOrderSaga();
        await InsertAndCommitAsync(instance);

        await using (var scope = _host!.Services.CreateAsyncScope())
        {
            var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
            var repository = scope.ServiceProvider
                .GetRequiredService<ISagaRepository<IntegrationOrderSaga>>();

            await unitOfWork.BeginAsync();
            var loaded = Assert.IsType<IntegrationOrderSaga>(
                await repository.GetForUpdateAsync(instance.CorrelationId, default));
            loaded.CurrentState = "Completed";
            loaded.OrderNumber = "ORD-UPDATED";
            loaded.AttemptNumber = 2;

            await repository.UpdateAsync(loaded, default);
            await unitOfWork.CommitAsync();
        }

        var updated = Assert.IsType<IntegrationOrderSaga>(
            await LoadAsync<IntegrationOrderSaga>(instance.CorrelationId));
        Assert.Equal("Completed", updated.CurrentState);
        Assert.Equal("ORD-UPDATED", updated.OrderNumber);
        Assert.Equal(2, updated.AttemptNumber);
    }

    [Fact]
    public async Task UpdateAsync_WhenTransactionRollsBack_KeepsPreviousStateAndData()
    {
        var instance = CreateOrderSaga();
        await InsertAndCommitAsync(instance);

        await using (var scope = _host!.Services.CreateAsyncScope())
        {
            var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
            var repository = scope.ServiceProvider
                .GetRequiredService<ISagaRepository<IntegrationOrderSaga>>();

            await unitOfWork.BeginAsync();
            var loaded = Assert.IsType<IntegrationOrderSaga>(
                await repository.GetForUpdateAsync(instance.CorrelationId, default));
            loaded.CurrentState = "RolledBackState";
            loaded.OrderNumber = "ROLLED-BACK";
            await repository.UpdateAsync(loaded, default);
            await unitOfWork.RollbackAsync();
        }

        var persisted = Assert.IsType<IntegrationOrderSaga>(
            await LoadAsync<IntegrationOrderSaga>(instance.CorrelationId));
        Assert.Equal(instance.CurrentState, persisted.CurrentState);
        Assert.Equal(instance.OrderNumber, persisted.OrderNumber);
    }

    [Fact]
    public async Task GetForUpdateAsync_UsesDatabaseColumnsAsIdentityAndStateSourceOfTruth()
    {
        var instance = CreateOrderSaga();
        await InsertAndCommitAsync(instance);
        const string stateStoredOnlyInColumn = "StateStoredOnlyInColumn";
        await using (var connection = new SqlConnection(_connectionString))
        {
            await connection.ExecuteAsync(
                """
                UPDATE [asyncomnia].[Saga]
                SET [CurrentState] = @CurrentState
                WHERE [CorrelationId] = @CorrelationId
                """,
                new
                {
                    CurrentState = stateStoredOnlyInColumn,
                    instance.CorrelationId
                });
        }

        var loaded = Assert.IsType<IntegrationOrderSaga>(
            await LoadAsync<IntegrationOrderSaga>(instance.CorrelationId));

        Assert.Equal(instance.CorrelationId, loaded.CorrelationId);
        Assert.Equal(stateStoredOnlyInColumn, loaded.CurrentState);
    }

    [Fact]
    public async Task GetForUpdateAsync_WhenSagaDoesNotExist_ReturnsNull()
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var repository = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<IntegrationOrderSaga>>();

        await unitOfWork.BeginAsync();
        var result = await repository.GetForUpdateAsync(Guid.NewGuid(), default);
        await unitOfWork.CommitAsync();

        Assert.Null(result);
    }

    [Fact]
    public async Task TryInsertAsync_WhenSagaAlreadyExists_ReturnsFalseAndKeepsTransactionUsable()
    {
        var instance = CreateOrderSaga();
        await InsertAndCommitAsync(instance);

        await using var scope = _host!.Services.CreateAsyncScope();
        var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var repository = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<IntegrationOrderSaga>>();

        await unitOfWork.BeginAsync();
        var inserted = await repository.TryInsertAsync(
            CreateOrderSaga(instance.CorrelationId),
            default);
        await unitOfWork.CommitAsync();

        Assert.False(inserted);
        Assert.Equal(1, await CountSagaRowsAsync());
    }

    [Fact]
    public async Task TryInsertAsync_AllowsSameCorrelationIdForDifferentSagaTypes()
    {
        var correlationId = Guid.NewGuid();
        await using var scope = _host!.Services.CreateAsyncScope();
        var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var orderRepository = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<IntegrationOrderSaga>>();
        var billingRepository = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<IntegrationBillingSaga>>();

        await unitOfWork.BeginAsync();
        var orderInserted = await orderRepository.TryInsertAsync(
            CreateOrderSaga(correlationId),
            default);
        var billingInserted = await billingRepository.TryInsertAsync(
            CreateBillingSaga(correlationId),
            default);
        await unitOfWork.CommitAsync();

        Assert.True(orderInserted);
        Assert.True(billingInserted);
        Assert.Equal(2, await CountSagaRowsAsync());

        var order = Assert.IsType<IntegrationOrderSaga>(
            await LoadAsync<IntegrationOrderSaga>(correlationId));
        var billing = Assert.IsType<IntegrationBillingSaga>(
            await LoadAsync<IntegrationBillingSaga>(correlationId));
        Assert.Equal("ORD-001", order.OrderNumber);
        Assert.Equal("billing-customer", billing.Customer);
    }

    [Fact]
    public async Task UpdateAsync_WhenSagaDoesNotExist_ThrowsInvalidOperationException()
    {
        var instance = CreateOrderSaga();
        await using var scope = _host!.Services.CreateAsyncScope();
        var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var repository = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<IntegrationOrderSaga>>();

        await unitOfWork.BeginAsync();
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            repository.UpdateAsync(instance, default));
        await unitOfWork.RollbackAsync();

        Assert.Contains(typeof(IntegrationOrderSaga).FullName!, exception.Message);
        Assert.Contains(instance.CorrelationId.ToString(), exception.Message);
    }

    [Fact]
    public async Task GetForUpdateAsync_WhenSagaIsLocked_WaitsAndReadsCommittedChanges()
    {
        var instance = CreateOrderSaga();
        await InsertAndCommitAsync(instance);
        await using var firstScope = _host!.Services.CreateAsyncScope();
        await using var secondScope = _host.Services.CreateAsyncScope();
        var firstUnitOfWork = firstScope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var secondUnitOfWork = secondScope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var firstRepository = firstScope.ServiceProvider
            .GetRequiredService<ISagaRepository<IntegrationOrderSaga>>();
        var secondRepository = secondScope.ServiceProvider
            .GetRequiredService<ISagaRepository<IntegrationOrderSaga>>();

        await firstUnitOfWork.BeginAsync();
        await secondUnitOfWork.BeginAsync();
        var locked = Assert.IsType<IntegrationOrderSaga>(
            await firstRepository.GetForUpdateAsync(instance.CorrelationId, default));
        locked.CurrentState = "ProcessedByFirstTransaction";
        await firstRepository.UpdateAsync(locked, default);

        var secondLoad = secondRepository.GetForUpdateAsync(instance.CorrelationId, default);
        await AssertTaskIsWaitingAsync(secondLoad);

        await firstUnitOfWork.CommitAsync();
        var loadedBySecond = Assert.IsType<IntegrationOrderSaga>(
            await secondLoad.WaitAsync(TimeSpan.FromSeconds(5)));

        Assert.Equal("ProcessedByFirstTransaction", loadedBySecond.CurrentState);
        await secondUnitOfWork.RollbackAsync();
    }

    [Fact]
    public async Task TryInsertAsync_WhenConcurrentInsertCommits_SecondInsertReturnsFalse()
    {
        var correlationId = Guid.NewGuid();
        await using var firstScope = _host!.Services.CreateAsyncScope();
        await using var secondScope = _host.Services.CreateAsyncScope();
        var firstUnitOfWork = firstScope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var secondUnitOfWork = secondScope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var firstRepository = firstScope.ServiceProvider
            .GetRequiredService<ISagaRepository<IntegrationOrderSaga>>();
        var secondRepository = secondScope.ServiceProvider
            .GetRequiredService<ISagaRepository<IntegrationOrderSaga>>();

        await firstUnitOfWork.BeginAsync();
        await secondUnitOfWork.BeginAsync();
        Assert.True(await firstRepository.TryInsertAsync(
            CreateOrderSaga(correlationId),
            default));

        var secondInsert = secondRepository.TryInsertAsync(
            CreateOrderSaga(correlationId),
            default);
        await AssertTaskIsWaitingAsync(secondInsert);

        await firstUnitOfWork.CommitAsync();
        Assert.False(await secondInsert.WaitAsync(TimeSpan.FromSeconds(5)));
        await secondUnitOfWork.CommitAsync();

        Assert.Equal(1, await CountSagaRowsAsync());
    }

    [Fact]
    public async Task TryInsertAsync_WhenConcurrentInsertRollsBack_SecondInsertSucceeds()
    {
        var correlationId = Guid.NewGuid();
        await using var firstScope = _host!.Services.CreateAsyncScope();
        await using var secondScope = _host.Services.CreateAsyncScope();
        var firstUnitOfWork = firstScope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var secondUnitOfWork = secondScope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var firstRepository = firstScope.ServiceProvider
            .GetRequiredService<ISagaRepository<IntegrationOrderSaga>>();
        var secondRepository = secondScope.ServiceProvider
            .GetRequiredService<ISagaRepository<IntegrationOrderSaga>>();

        await firstUnitOfWork.BeginAsync();
        await secondUnitOfWork.BeginAsync();
        Assert.True(await firstRepository.TryInsertAsync(
            CreateOrderSaga(correlationId),
            default));

        var secondInsert = secondRepository.TryInsertAsync(
            CreateOrderSaga(correlationId),
            default);
        await AssertTaskIsWaitingAsync(secondInsert);

        await firstUnitOfWork.RollbackAsync();
        Assert.True(await secondInsert.WaitAsync(TimeSpan.FromSeconds(5)));
        await secondUnitOfWork.CommitAsync();

        Assert.Equal(1, await CountSagaRowsAsync());
    }

    public async Task InitializeAsync()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddScoped<IIntegrationUnitOfWork>(_ =>
            new IntegrationUnitOfWork(_connectionString));

        builder.Services
            .AddAsyncomniaMessaging()
            .UseSqlServer(options => options.ConnectionString = _connectionString)
            .UseAdoNet<IIntegrationUnitOfWork>();

        _host = builder.Build();

        await _host.MigrateAsyncomniaAsync();
        await ExecuteNonQueryAsync("DELETE FROM [asyncomnia].[Saga]");
    }

    public Task DisposeAsync()
    {
        _host?.Dispose();
        return Task.CompletedTask;
    }

    private async Task<bool> InsertAndCommitAsync<TSagaInstance>(
        TSagaInstance instance)
        where TSagaInstance : AsyncomniaSagaInstance
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var repository = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<TSagaInstance>>();

        await unitOfWork.BeginAsync();
        var inserted = await repository.TryInsertAsync(instance, default);
        await unitOfWork.CommitAsync();

        return inserted;
    }

    private async Task<TSagaInstance?> LoadAsync<TSagaInstance>(Guid correlationId)
        where TSagaInstance : AsyncomniaSagaInstance
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var unitOfWork = scope.ServiceProvider.GetRequiredService<IIntegrationUnitOfWork>();
        var repository = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<TSagaInstance>>();

        await unitOfWork.BeginAsync();
        var instance = await repository.GetForUpdateAsync(correlationId, default);
        await unitOfWork.CommitAsync();

        return instance;
    }

    private async Task<SagaRow> GetSagaRowAsync(Guid correlationId)
    {
        const string sql = """
            SELECT
                [SagaType],
                [CorrelationId],
                [CurrentState],
                [DataJson],
                [CreatedAt],
                [UpdatedAt]
            FROM [asyncomnia].[Saga]
            WHERE [CorrelationId] = @CorrelationId
            """;
        await using var connection = new SqlConnection(_connectionString);

        return await connection.QuerySingleAsync<SagaRow>(sql, new { CorrelationId = correlationId });
    }

    private Task<int> CountSagaRowsAsync()
    {
        return ExecuteScalarAsync<int>("SELECT COUNT(*) FROM [asyncomnia].[Saga]");
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

    private static async Task AssertTaskIsWaitingAsync(Task task)
    {
        var delay = Task.Delay(TimeSpan.FromMilliseconds(200));
        var completed = await Task.WhenAny(task, delay);

        Assert.NotSame(task, completed);
    }

    private static IntegrationOrderSaga CreateOrderSaga(Guid? correlationId = null)
    {
        return new IntegrationOrderSaga
        {
            CorrelationId = correlationId ?? Guid.NewGuid(),
            CurrentState = "Submitted",
            OrderNumber = "ORD-001",
            AttemptNumber = 1
        };
    }

    private static IntegrationBillingSaga CreateBillingSaga(Guid correlationId)
    {
        return new IntegrationBillingSaga
        {
            CorrelationId = correlationId,
            CurrentState = "AwaitingPayment",
            Customer = "billing-customer"
        };
    }

    private sealed class SagaRow
    {
        public string SagaType { get; init; } = null!;
        public Guid CorrelationId { get; init; }
        public string CurrentState { get; init; } = null!;
        public string DataJson { get; init; } = null!;
        public DateTimeOffset CreatedAt { get; init; }
        public DateTimeOffset UpdatedAt { get; init; }
    }
}

public sealed class IntegrationOrderSaga : AsyncomniaSagaInstance
{
    public string OrderNumber { get; set; } = null!;
    public int AttemptNumber { get; set; }
}

public sealed class IntegrationBillingSaga : AsyncomniaSagaInstance
{
    public string Customer { get; set; } = null!;
}
