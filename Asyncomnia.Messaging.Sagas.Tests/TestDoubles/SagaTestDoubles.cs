using Asyncomnia.Messaging.Sagas.StateMachine;
using Asyncomnia.Messaging.Sagas.StateMachine.Behavior;
using Asyncomnia.Messaging.Sagas.StateMachine.Configuration;
using Asyncomnia.Messaging.Sagas.Persistence;

namespace Asyncomnia.Messaging.Sagas.Tests.TestDoubles;

internal sealed record TestEvent(Guid SagaId, string Value = "value") : IEvent;

internal sealed record SecondTestEvent(Guid SagaId) : IEvent;

internal sealed record TestCommand(Guid SagaId, string Value = "value") : ICommand;

internal sealed record ScheduledTimeout : IEvent;

internal sealed class TestSagaInstance : AsyncomniaSagaInstance
{
    public bool Condition { get; set; }
    public List<string> ExecutedActivities { get; } = [];
}

internal sealed class TestStateMachine : AsyncomniaStateMachine<TestSagaInstance>
{
    public State ConfigureState(string name)
    {
        return State(name);
    }

    public SagaEvent<TMessage> ConfigureEvent<TMessage>(
        Action<SagaEventConfiguration<TMessage>> configuration)
        where TMessage : class, IMessage
    {
        return Event(configuration);
    }

    public SagaBehaviorConfiguration<TestSagaInstance, TMessage> ConfigureWhen<TMessage>(
        SagaEvent<TMessage> @event)
        where TMessage : class, IMessage
    {
        return When(@event);
    }

    public void ConfigureInitially(params SagaBehaviorConfiguration<TestSagaInstance>[] behaviors)
    {
        Initially(behaviors);
    }

    public void ConfigureDuring(
        State state,
        params SagaBehaviorConfiguration<TestSagaInstance>[] behaviors)
    {
        During(state, behaviors);
    }
}

internal sealed class TestServiceProvider : IServiceProvider
{
    private readonly Dictionary<Type, object> _services = new();

    public CapturingEventPublisher EventPublisher { get; } = new();
    public CapturingCommandSender CommandSender { get; } = new();
    public CapturingMessageScheduler MessageScheduler { get; } = new();

    public TestServiceProvider(
        bool includeEventPublisher = true,
        bool includeCommandSender = true,
        bool includeMessageScheduler = false)
    {
        if (includeEventPublisher)
        {
            Add<IEventPublisher>(EventPublisher);
        }

        if (includeCommandSender)
        {
            Add<ICommandSender>(CommandSender);
        }

        if (includeMessageScheduler)
        {
            Add<IMessageScheduler>(MessageScheduler);
        }
    }

    public void Add<TService>(TService service) where TService : class
    {
        _services[typeof(TService)] = service;
    }

    public object? GetService(Type serviceType)
    {
        return _services.GetValueOrDefault(serviceType);
    }
}

internal sealed class CapturingEventPublisher : IEventPublisher
{
    public object? PublishedEvent { get; private set; }
    public MessageDispatchOptions? Options { get; private set; }
    public CancellationToken CancellationToken { get; private set; }
    public int Calls { get; private set; }

    public Task PublishAsync<TEvent>(
        TEvent @event,
        MessageDispatchOptions? options = null,
        CancellationToken token = default)
        where TEvent : class, IEvent
    {
        PublishedEvent = @event;
        Options = options;
        CancellationToken = token;
        Calls++;

        return Task.CompletedTask;
    }
}

internal sealed class CapturingCommandSender : ICommandSender
{
    public object? SentCommand { get; private set; }
    public MessageDispatchOptions? Options { get; private set; }
    public CancellationToken CancellationToken { get; private set; }
    public int Calls { get; private set; }

    public Task SendAsync<TCommand>(
        TCommand command,
        MessageDispatchOptions? options = null,
        CancellationToken token = default)
        where TCommand : class, ICommand
    {
        SentCommand = command;
        Options = options;
        CancellationToken = token;
        Calls++;

        return Task.CompletedTask;
    }
}

internal sealed class CapturingMessageScheduler : IMessageScheduler
{
    public Guid ScheduleId { get; set; } = Guid.NewGuid();
    public bool CancelResult { get; set; } = true;
    public object? ScheduledMessage { get; private set; }
    public DateTimeOffset? DeliverAt { get; private set; }
    public TimeSpan? Delay { get; private set; }
    public MessageDispatchOptions? Options { get; private set; }
    public CancellationToken CancellationToken { get; private set; }
    public Guid? CancelledScheduleId { get; private set; }
    public CancellationToken CancellationTokenAtCancellation { get; private set; }
    public int ScheduleCalls { get; private set; }
    public int CancelCalls { get; private set; }

    public Task<Guid> ScheduleAsync<TMessage>(
        TMessage message,
        DateTimeOffset deliverAt,
        MessageDispatchOptions? options = null,
        CancellationToken token = default)
        where TMessage : class, IMessage
    {
        ScheduledMessage = message;
        DeliverAt = deliverAt;
        Options = options;
        CancellationToken = token;
        ScheduleCalls++;

        return Task.FromResult(ScheduleId);
    }

    public Task<Guid> ScheduleAsync<TMessage>(
        TMessage message,
        TimeSpan delay,
        MessageDispatchOptions? options = null,
        CancellationToken token = default)
        where TMessage : class, IMessage
    {
        ScheduledMessage = message;
        Delay = delay;
        Options = options;
        CancellationToken = token;
        ScheduleCalls++;

        return Task.FromResult(ScheduleId);
    }

    public Task<bool> CancelAsync(Guid scheduleId, CancellationToken token = default)
    {
        CancelledScheduleId = scheduleId;
        CancellationTokenAtCancellation = token;
        CancelCalls++;

        return Task.FromResult(CancelResult);
    }
}

internal sealed class ScopedDependency;

internal sealed class StubSagaRepository<TSagaInstance> : ISagaRepository<TSagaInstance>
    where TSagaInstance : AsyncomniaSagaInstance
{
    private readonly Queue<TSagaInstance?> _getResults = new();

    public List<(Guid CorrelationId, CancellationToken CancellationToken)> GetCalls { get; } = [];
    public List<(TSagaInstance Instance, CancellationToken CancellationToken)> InsertCalls { get; } = [];
    public List<(TSagaInstance Instance, CancellationToken CancellationToken)> UpdateCalls { get; } = [];

    public bool InsertResult { get; set; } = true;

    public void EnqueueGetResult(TSagaInstance? instance)
    {
        _getResults.Enqueue(instance);
    }

    public Task<TSagaInstance?> GetForUpdateAsync(Guid correlationId, CancellationToken token)
    {
        GetCalls.Add((correlationId, token));

        var result = _getResults.Count > 0 ? _getResults.Dequeue() : null;
        return Task.FromResult(result);
    }

    public Task<bool> TryInsertAsync(TSagaInstance instance, CancellationToken token)
    {
        InsertCalls.Add((instance, token));
        return Task.FromResult(InsertResult);
    }

    public Task UpdateAsync(TSagaInstance instance, CancellationToken token)
    {
        UpdateCalls.Add((instance, token));
        return Task.CompletedTask;
    }
}
