using System.Text.Json;
using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.Infrastructure;
using Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.TestDoubles;
using Asyncomnia.Messaging.Sagas.DependencyInjection;
using Asyncomnia.Messaging.Sagas.Persistence;
using Asyncomnia.Messaging.Sagas.StateMachine;
using Asyncomnia.Messaging.Sagas.StateMachine.Configuration;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.Sagas;

[Collection(SqlServerCollection.Name)]
public sealed class SqlServerSagaRuntimeIntegrationTests : IAsyncLifetime
{
    private const string ConsumerId =
        "events/orders/subscription/saga-integration-tests";

    private static readonly JsonSerializerOptions SerializerOptions =
        new(JsonSerializerDefaults.Web);

    private readonly string _connectionString;
    private readonly SagaConcurrencyProbe _concurrencyProbe = new();
    private IHost? _host;

    public SqlServerSagaRuntimeIntegrationTests(
        SqlServerContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task HandleEventAsync_WhenSagaIsNew_CommitsInboxSagaDomainAndOutboxTogether()
    {
        var sagaId = Guid.NewGuid();
        var orderId = Guid.NewGuid();

        await DispatchEventAsync(
            Guid.NewGuid(),
            new StartIntegrationOrderSaga(sagaId, orderId, "customer-1"));

        var saga = Assert.IsType<IntegrationRuntimeSaga>(
            await LoadSagaAsync(sagaId));
        Assert.Equal(IntegrationRuntimeStateMachine.ActiveStateName, saga.CurrentState);
        Assert.Equal("customer-1", saga.Customer);
        Assert.Equal(1, saga.StartedCount);
        Assert.Equal(1, await CountAsync("[asyncomnia].[Inbox]"));
        Assert.Equal(1, await CountAsync("[asyncomnia].[Saga]"));
        Assert.Equal(1, await CountAsync("[dbo].[Orders]"));
        Assert.Equal(1, await CountAsync("[asyncomnia].[Outbox]"));

        var outboxMessage = await LoadSingleOutboxMessageAsync();
        Assert.Equal(sagaId, outboxMessage.CorrelationId);
        Assert.Equal(GetContractType<IntegrationSagaProgressed>(), outboxMessage.ContractType);

        var publishedEvent = Assert.IsType<IntegrationSagaProgressed>(
            JsonSerializer.Deserialize<IntegrationSagaProgressed>(
                outboxMessage.MessageJson,
                SerializerOptions));
        Assert.Equal(sagaId, publishedEvent.SagaId);
        Assert.Equal("started", publishedEvent.Stage);
    }

    [Fact]
    public async Task HandleCommandAsync_WhenSagaExists_LoadsAndFinalizesPersistedSaga()
    {
        var sagaId = Guid.NewGuid();

        await DispatchEventAsync(
            Guid.NewGuid(),
            new StartIntegrationOrderSaga(
                sagaId,
                Guid.NewGuid(),
                "customer-2"));

        await DispatchCommandAsync(
            Guid.NewGuid(),
            new CompleteIntegrationOrderSaga(sagaId, "approved"));

        var saga = Assert.IsType<IntegrationRuntimeSaga>(
            await LoadSagaAsync(sagaId));
        Assert.Equal(State.Final.Name, saga.CurrentState);
        Assert.Equal("approved", saga.CompletionReason);
        Assert.Equal(1, saga.StartedCount);
        Assert.Equal(2, await CountAsync("[asyncomnia].[Inbox]"));
        Assert.Equal(1, await CountAsync("[asyncomnia].[Saga]"));
        Assert.Equal(2, await CountAsync("[asyncomnia].[Outbox]"));
    }

    [Fact]
    public async Task HandleEventAsync_WhenInitialDeliveryIsDuplicated_ExecutesSagaOnce()
    {
        var messageId = Guid.NewGuid();
        var sagaId = Guid.NewGuid();
        var message = new StartIntegrationOrderSaga(
            sagaId,
            Guid.NewGuid(),
            "duplicate-customer");

        await DispatchEventAsync(messageId, message);
        await DispatchEventAsync(messageId, message);

        var saga = Assert.IsType<IntegrationRuntimeSaga>(
            await LoadSagaAsync(sagaId));
        Assert.Equal(1, saga.StartedCount);
        Assert.Equal(1, await CountAsync("[asyncomnia].[Inbox]"));
        Assert.Equal(1, await CountAsync("[asyncomnia].[Saga]"));
        Assert.Equal(1, await CountAsync("[dbo].[Orders]"));
        Assert.Equal(1, await CountAsync("[asyncomnia].[Outbox]"));
    }

    [Fact]
    public async Task HandleEventAsync_WhenSagaBehaviorFails_RollsBackAllTransactionalChanges()
    {
        var sagaId = Guid.NewGuid();

        var exception = await Assert.ThrowsAsync<SagaIntegrationException>(() =>
            DispatchEventAsync(
                Guid.NewGuid(),
                new FailIntegrationOrderSaga(
                    sagaId,
                    Guid.NewGuid())));

        Assert.Equal("Simulated saga behavior failure.", exception.Message);
        Assert.Null(await LoadSagaAsync(sagaId));
        Assert.Equal(0, await CountAsync("[asyncomnia].[Inbox]"));
        Assert.Equal(0, await CountAsync("[asyncomnia].[Saga]"));
        Assert.Equal(0, await CountAsync("[dbo].[Orders]"));
        Assert.Equal(0, await CountAsync("[asyncomnia].[Outbox]"));
    }

    [Fact]
    public async Task HandleCommandAsync_WhenTwoMessagesTargetSameSaga_SerializesUpdatesWithoutLosingData()
    {
        var sagaId = Guid.NewGuid();

        await DispatchEventAsync(
            Guid.NewGuid(),
            new StartIntegrationOrderSaga(
                sagaId,
                Guid.NewGuid(),
                "concurrent-customer"));

        var firstDispatch = DispatchCommandAsync(
            Guid.NewGuid(),
            new IncrementIntegrationSaga(sagaId));

        await _concurrencyProbe.FirstExecutionEntered
            .WaitAsync(TimeSpan.FromSeconds(5));

        var secondDispatch = DispatchCommandAsync(
            Guid.NewGuid(),
            new IncrementIntegrationSaga(sagaId));

        try
        {
            await AssertTaskIsWaitingAsync(secondDispatch);
            Assert.Equal(1, _concurrencyProbe.ExecutionCount);
        }
        finally
        {
            _concurrencyProbe.ReleaseFirstExecution();
        }

        await Task.WhenAll(firstDispatch, secondDispatch)
            .WaitAsync(TimeSpan.FromSeconds(10));

        var saga = Assert.IsType<IntegrationRuntimeSaga>(
            await LoadSagaAsync(sagaId));
        Assert.Equal(2, saga.ProcessedCommands);
        Assert.Equal(2, _concurrencyProbe.ExecutionCount);
        Assert.Equal(3, await CountAsync("[asyncomnia].[Inbox]"));
        Assert.Equal(1, await CountAsync("[asyncomnia].[Saga]"));
    }

    public async Task InitializeAsync()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddSingleton(_concurrencyProbe);
        builder.Services.AddScoped<IIntegrationUnitOfWork>(_ =>
            new IntegrationUnitOfWork(_connectionString));

        builder.Services
            .AddAsyncomniaMessaging()
            .RegisterAsyncomniaSaga<IntegrationRuntimeStateMachine, IntegrationRuntimeSaga>()
            .UseSqlServer(options =>
                options.ConnectionString = _connectionString)
            .UseAdoNet<IIntegrationUnitOfWork>()
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

    private async Task DispatchEventAsync<TEvent>(
        Guid messageId,
        TEvent message)
        where TEvent : class, IEvent
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var pipeline = scope.ServiceProvider
            .GetRequiredService<IInboundMessagePipeline>();
        var context = CreateInboundContext(messageId, message);

        await pipeline.HandleEventAsync<TEvent>(context);
    }

    private async Task DispatchCommandAsync<TCommand>(
        Guid messageId,
        TCommand message)
        where TCommand : class, ICommand
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var pipeline = scope.ServiceProvider
            .GetRequiredService<IInboundMessagePipeline>();
        var context = CreateInboundContext(messageId, message);

        await pipeline.HandleCommandAsync<TCommand>(context);
    }

    private static InboundMessageContext CreateInboundContext<TMessage>(
        Guid messageId,
        TMessage message)
        where TMessage : class, IMessage
    {
        return new InboundMessageContext(
            ConsumerId,
            GetContractType<TMessage>(),
            message,
            new MessageMetadata(messageId, DateTimeOffset.UtcNow));
    }

    private async Task<IntegrationRuntimeSaga?> LoadSagaAsync(
        Guid correlationId)
    {
        await using var scope = _host!.Services.CreateAsyncScope();
        var unitOfWork = scope.ServiceProvider
            .GetRequiredService<IIntegrationUnitOfWork>();
        var repository = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<IntegrationRuntimeSaga>>();

        await unitOfWork.BeginAsync();
        var saga = await repository.GetForUpdateAsync(correlationId, default);
        await unitOfWork.CommitAsync();

        return saga;
    }

    private async Task<OutboxRow> LoadSingleOutboxMessageAsync()
    {
        const string sql = """
            SELECT [ContractType], [MessageJson], [CorrelationId]
            FROM [asyncomnia].[Outbox]
            """;

        await using var connection = new SqlConnection(_connectionString);
        return await connection.QuerySingleAsync<OutboxRow>(sql);
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
            DELETE FROM [asyncomnia].[Saga];
            DELETE FROM [dbo].[Orders];
            """;

        await ExecuteNonQueryAsync(sql);
    }

    private async Task<int> CountAsync(string tableName)
    {
        await using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<int>(
            $"SELECT COUNT(*) FROM {tableName}");
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

    private static string GetContractType<TMessage>()
    {
        var type = typeof(TMessage);
        return $"{type.FullName}, {type.Assembly.GetName().Name}";
    }

    private sealed class OutboxRow
    {
        public string ContractType { get; init; } = null!;
        public string MessageJson { get; init; } = null!;
        public Guid? CorrelationId { get; init; }
    }
}

public sealed record StartIntegrationOrderSaga(
    Guid SagaId,
    Guid OrderId,
    string Customer) : IEvent;

public sealed record CompleteIntegrationOrderSaga(
    Guid SagaId,
    string Reason) : ICommand;

public sealed record IncrementIntegrationSaga(Guid SagaId) : ICommand;

public sealed record FailIntegrationOrderSaga(
    Guid SagaId,
    Guid OrderId) : IEvent;

public sealed record IntegrationSagaProgressed(
    Guid SagaId,
    string Stage) : IEvent;

public sealed class IntegrationRuntimeSaga : AsyncomniaSagaInstance
{
    public string Customer { get; set; } = null!;
    public string? CompletionReason { get; set; }
    public int StartedCount { get; set; }
    public int ProcessedCommands { get; set; }
}

public sealed class IntegrationRuntimeStateMachine
    : AsyncomniaStateMachine<IntegrationRuntimeSaga>
{
    public const string ActiveStateName = "Active";

    public IntegrationRuntimeStateMachine()
    {
        var active = State(ActiveStateName);
        var started = Event<StartIntegrationOrderSaga>(configuration =>
            configuration.CorrelateById(message => message.SagaId));
        var completed = Event<CompleteIntegrationOrderSaga>(configuration =>
            configuration.CorrelateById(message => message.SagaId));
        var incremented = Event<IncrementIntegrationSaga>(configuration =>
            configuration.CorrelateById(message => message.SagaId));
        var failed = Event<FailIntegrationOrderSaga>(configuration =>
            configuration.CorrelateById(message => message.SagaId));

        Initially(
            When(started)
                .Then(context =>
                {
                    context.State.Customer = context.Message.Customer;
                    context.State.StartedCount++;
                })
                .ThenAsync(async context =>
                {
                    var unitOfWork = context.Services
                        .GetRequiredService<IIntegrationUnitOfWork>();

                    await unitOfWork.InsertOrderAsync(
                        context.Message.OrderId,
                        "Created by saga",
                        context.CancellationToken);

                    await context.PublishAsync(
                        new IntegrationSagaProgressed(
                            context.State.CorrelationId,
                            "started"));
                })
                .TransitionTo(active),
            When(failed)
                .ThenAsync(async context =>
                {
                    var unitOfWork = context.Services
                        .GetRequiredService<IIntegrationUnitOfWork>();

                    await unitOfWork.InsertOrderAsync(
                        context.Message.OrderId,
                        "Must be rolled back",
                        context.CancellationToken);

                    await context.PublishAsync(
                        new IntegrationSagaProgressed(
                            context.State.CorrelationId,
                            "must-be-rolled-back"));

                    throw new SagaIntegrationException(
                        "Simulated saga behavior failure.");
                }));

        During(
            active,
            When(completed)
                .Then(context =>
                    context.State.CompletionReason = context.Message.Reason)
                .ThenAsync(context =>
                    context.PublishAsync(
                        new IntegrationSagaProgressed(
                            context.State.CorrelationId,
                            "completed")))
                .Finalize(),
            When(incremented)
                .ThenAsync(async context =>
                {
                    var probe = context.Services
                        .GetRequiredService<SagaConcurrencyProbe>();

                    await probe.WaitAsync(context.CancellationToken);
                    context.State.ProcessedCommands++;
                }));
    }
}

public sealed class SagaConcurrencyProbe
{
    private readonly TaskCompletionSource _firstExecutionEntered =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly TaskCompletionSource _releaseFirstExecution =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private int _executionCount;

    public Task FirstExecutionEntered => _firstExecutionEntered.Task;
    public int ExecutionCount => Volatile.Read(ref _executionCount);

    public async Task WaitAsync(CancellationToken token)
    {
        var executionNumber = Interlocked.Increment(ref _executionCount);

        if (executionNumber != 1)
        {
            return;
        }

        _firstExecutionEntered.TrySetResult();
        await _releaseFirstExecution.Task.WaitAsync(token);
    }

    public void ReleaseFirstExecution()
    {
        _releaseFirstExecution.TrySetResult();
    }
}

public sealed class SagaIntegrationException(string message)
    : Exception(message);
