using Asyncomnia.Messaging.Sagas.Runtime;
using Asyncomnia.Messaging.Sagas.StateMachine;
using Asyncomnia.Messaging.Sagas.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Sagas.Tests.Runtime;

public sealed class SagaRuntimeTests
{
    [Fact]
    public void Constructor_WhenRepositoryIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new SagaRuntime<RuntimeStateMachine, TestSagaInstance>(
                null!,
                new RuntimeStateMachine(),
                new TestServiceProvider()));

        Assert.Equal("sagaRepository", exception.ParamName);
    }

    [Fact]
    public void Constructor_WhenStateMachineIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new SagaRuntime<RuntimeStateMachine, TestSagaInstance>(
                new StubSagaRepository<TestSagaInstance>(),
                null!,
                new TestServiceProvider()));

        Assert.Equal("stateMachine", exception.ParamName);
    }

    [Fact]
    public void Constructor_WhenServiceProviderIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new SagaRuntime<RuntimeStateMachine, TestSagaInstance>(
                new StubSagaRepository<TestSagaInstance>(),
                new RuntimeStateMachine(),
                null!));

        Assert.Equal("serviceProvider", exception.ParamName);
    }

    [Fact]
    public async Task HandleAsync_WhenMessageIsNull_ThrowsArgumentNullException()
    {
        var runtime = CreateRuntime(new StubSagaRepository<TestSagaInstance>());

        var exception = await Assert.ThrowsAsync<ArgumentNullException>(() =>
            runtime.HandleAsync(null!, CreateMetadata()));

        Assert.Equal("message", exception.ParamName);
    }

    [Fact]
    public async Task HandleAsync_WhenMetadataIsNull_ThrowsArgumentNullException()
    {
        var runtime = CreateRuntime(new StubSagaRepository<TestSagaInstance>());

        var exception = await Assert.ThrowsAsync<ArgumentNullException>(() =>
            runtime.HandleAsync(new TestEvent(Guid.NewGuid()), null!));

        Assert.Equal("metadata", exception.ParamName);
    }

    [Fact]
    public async Task HandleAsync_WhenMessageIsNotRegistered_ThrowsBeforeAccessingRepository()
    {
        var repository = new StubSagaRepository<TestSagaInstance>();
        var runtime = CreateRuntime(repository);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            runtime.HandleAsync(new SecondTestEvent(Guid.NewGuid()), CreateMetadata()));

        Assert.Contains(typeof(RuntimeStateMachine).FullName!, exception.Message);
        Assert.Contains(typeof(SecondTestEvent).FullName!, exception.Message);
        Assert.Empty(repository.GetCalls);
        Assert.Empty(repository.InsertCalls);
        Assert.Empty(repository.UpdateCalls);
    }

    [Fact]
    public async Task HandleAsync_WhenInstanceExists_ExecutesCurrentStateBehaviorAndUpdatesInstance()
    {
        var correlationId = Guid.NewGuid();
        var instance = new TestSagaInstance
        {
            CorrelationId = correlationId,
            CurrentState = RuntimeStateMachine.ActiveStateName
        };
        var repository = new StubSagaRepository<TestSagaInstance>();
        repository.EnqueueGetResult(instance);
        var runtime = CreateRuntime(repository);
        using var cancellationTokenSource = new CancellationTokenSource();

        await runtime.HandleAsync(
            new TestEvent(correlationId, "existing"),
            CreateMetadata(),
            cancellationTokenSource.Token);

        var getCall = Assert.Single(repository.GetCalls);
        Assert.Equal(correlationId, getCall.CorrelationId);
        Assert.Equal(cancellationTokenSource.Token, getCall.CancellationToken);
        Assert.Empty(repository.InsertCalls);
        var updateCall = Assert.Single(repository.UpdateCalls);
        Assert.Same(instance, updateCall.Instance);
        Assert.Equal(cancellationTokenSource.Token, updateCall.CancellationToken);
        Assert.Equal(new[] { "active:existing" }, instance.ExecutedActivities);
        Assert.Equal(RuntimeStateMachine.ActiveStateName, instance.CurrentState);
    }

    [Fact]
    public async Task HandleAsync_WhenInstanceDoesNotExist_InsertsExecutesAndUpdatesNewInstance()
    {
        var correlationId = Guid.NewGuid();
        var repository = new StubSagaRepository<TestSagaInstance>();
        repository.EnqueueGetResult(null);
        var runtime = CreateRuntime(repository);

        await runtime.HandleAsync(
            new TestEvent(correlationId, "new"),
            CreateMetadata());

        var insertCall = Assert.Single(repository.InsertCalls);
        Assert.Equal(correlationId, insertCall.Instance.CorrelationId);
        Assert.Equal(new[] { "initial:new" }, insertCall.Instance.ExecutedActivities);
        Assert.Equal(RuntimeStateMachine.ActiveStateName, insertCall.Instance.CurrentState);

        var updateCall = Assert.Single(repository.UpdateCalls);
        Assert.Same(insertCall.Instance, updateCall.Instance);
    }

    [Fact]
    public async Task HandleAsync_WhenInsertLosesRace_LoadsWinnerAndProcessesIt()
    {
        var correlationId = Guid.NewGuid();
        var winner = new TestSagaInstance
        {
            CorrelationId = correlationId
        };
        var repository = new StubSagaRepository<TestSagaInstance>
        {
            InsertResult = false
        };
        repository.EnqueueGetResult(null);
        repository.EnqueueGetResult(winner);
        var runtime = CreateRuntime(repository);

        await runtime.HandleAsync(
            new TestEvent(correlationId, "winner"),
            CreateMetadata());

        Assert.Equal(2, repository.GetCalls.Count);
        Assert.All(repository.GetCalls, call => Assert.Equal(correlationId, call.CorrelationId));
        Assert.Single(repository.InsertCalls);
        var updateCall = Assert.Single(repository.UpdateCalls);
        Assert.Same(winner, updateCall.Instance);
        Assert.Equal(new[] { "initial:winner" }, winner.ExecutedActivities);
        Assert.Equal(RuntimeStateMachine.ActiveStateName, winner.CurrentState);
    }

    [Fact]
    public async Task HandleAsync_WhenInsertLosesRaceAndWinnerCannotBeLoaded_ThrowsDescriptiveException()
    {
        var correlationId = Guid.NewGuid();
        var repository = new StubSagaRepository<TestSagaInstance>
        {
            InsertResult = false
        };
        repository.EnqueueGetResult(null);
        repository.EnqueueGetResult(null);
        var runtime = CreateRuntime(repository);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            runtime.HandleAsync(new TestEvent(correlationId), CreateMetadata()));

        Assert.Contains(typeof(TestSagaInstance).FullName!, exception.Message);
        Assert.Contains(correlationId.ToString(), exception.Message);
        Assert.Equal(2, repository.GetCalls.Count);
        Assert.Single(repository.InsertCalls);
        Assert.Empty(repository.UpdateCalls);
    }

    [Fact]
    public async Task HandleAsync_WhenCurrentStateHasNoBehavior_ThrowsWithoutUpdatingInstance()
    {
        var correlationId = Guid.NewGuid();
        var instance = new TestSagaInstance
        {
            CorrelationId = correlationId
        };
        var repository = new StubSagaRepository<TestSagaInstance>();
        repository.EnqueueGetResult(instance);
        var runtime = new SagaRuntime<EventWithoutBehaviorStateMachine, TestSagaInstance>(
            repository,
            new EventWithoutBehaviorStateMachine(),
            new TestServiceProvider());

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            runtime.HandleAsync(new TestEvent(correlationId), CreateMetadata()));

        Assert.Contains(typeof(EventWithoutBehaviorStateMachine).FullName!, exception.Message);
        Assert.Contains(typeof(TestEvent).FullName!, exception.Message);
        Assert.Contains(instance.CurrentState, exception.Message);
        Assert.Contains(correlationId.ToString(), exception.Message);
        Assert.Empty(repository.UpdateCalls);
    }

    [Fact]
    public async Task HandleAsync_WhenBehaviorThrows_DoesNotUpdateInstanceAndPropagatesException()
    {
        var correlationId = Guid.NewGuid();
        var instance = new TestSagaInstance
        {
            CorrelationId = correlationId
        };
        var repository = new StubSagaRepository<TestSagaInstance>();
        repository.EnqueueGetResult(instance);
        var runtime = new SagaRuntime<ThrowingStateMachine, TestSagaInstance>(
            repository,
            new ThrowingStateMachine(),
            new TestServiceProvider());

        var exception = await Assert.ThrowsAsync<TestBehaviorException>(() =>
            runtime.HandleAsync(new TestEvent(correlationId), CreateMetadata()));

        Assert.Equal("behavior failed", exception.Message);
        Assert.Empty(repository.UpdateCalls);
    }

    [Fact]
    public async Task HandleAsync_WithMessageCorrelationStrategy_UsesMetadataCorrelationId()
    {
        var correlationId = Guid.NewGuid();
        var repository = new StubSagaRepository<TestSagaInstance>();
        repository.EnqueueGetResult(null);
        var runtime = new SagaRuntime<MetadataCorrelationStateMachine, TestSagaInstance>(
            repository,
            new MetadataCorrelationStateMachine(),
            new TestServiceProvider());

        await runtime.HandleAsync(
            new ScheduledTimeout(),
            CreateMetadata(correlationId));

        Assert.Equal(correlationId, Assert.Single(repository.GetCalls).CorrelationId);
        Assert.Equal(correlationId, Assert.Single(repository.InsertCalls).Instance.CorrelationId);
        Assert.Single(repository.UpdateCalls);
    }

    private static SagaRuntime<RuntimeStateMachine, TestSagaInstance> CreateRuntime(
        StubSagaRepository<TestSagaInstance> repository)
    {
        return new SagaRuntime<RuntimeStateMachine, TestSagaInstance>(
            repository,
            new RuntimeStateMachine(),
            new TestServiceProvider());
    }

    private static MessageMetadata CreateMetadata(Guid? correlationId = null)
    {
        return new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow)
        {
            CorrelationId = correlationId
        };
    }

    private sealed class RuntimeStateMachine : AsyncomniaStateMachine<TestSagaInstance>
    {
        public const string ActiveStateName = "Active";

        public RuntimeStateMachine()
        {
            var active = State(ActiveStateName);
            var testEvent = Event<TestEvent>(configuration =>
                configuration.CorrelateById(message => message.SagaId));

            Initially(
                When(testEvent)
                    .Then(context => context.State.ExecutedActivities.Add($"initial:{context.Message.Value}"))
                    .TransitionTo(active));

            During(
                active,
                When(testEvent)
                    .Then(context => context.State.ExecutedActivities.Add($"active:{context.Message.Value}")));
        }
    }

    private sealed class EventWithoutBehaviorStateMachine : AsyncomniaStateMachine<TestSagaInstance>
    {
        public EventWithoutBehaviorStateMachine()
        {
            Event<TestEvent>(configuration =>
                configuration.CorrelateById(message => message.SagaId));
        }
    }

    private sealed class ThrowingStateMachine : AsyncomniaStateMachine<TestSagaInstance>
    {
        public ThrowingStateMachine()
        {
            var testEvent = Event<TestEvent>(configuration =>
                configuration.CorrelateById(message => message.SagaId));

            Initially(
                When(testEvent)
                    .Then(_ => throw new TestBehaviorException("behavior failed")));
        }
    }

    private sealed class MetadataCorrelationStateMachine : AsyncomniaStateMachine<TestSagaInstance>
    {
        public MetadataCorrelationStateMachine()
        {
            var timeout = Event<ScheduledTimeout>(configuration =>
                configuration.CorrelateByMessageCorrelationId());

            Initially(When(timeout));
        }
    }

    private sealed class TestBehaviorException(string message) : Exception(message);
}
