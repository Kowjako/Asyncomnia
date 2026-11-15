using Asyncomnia.Messaging.Sagas.StateMachine;
using Asyncomnia.Messaging.Sagas.StateMachine.Behavior;
using Asyncomnia.Messaging.Sagas.StateMachine.Configuration;

namespace Asyncomnia.Messaging.Sagas.Tests.TestDoubles;

internal sealed record TestEvent(Guid SagaId, string Value = "value") : IEvent;

internal sealed record SecondTestEvent(Guid SagaId) : IEvent;

internal sealed record TestCommand(Guid SagaId, string Value = "value") : ICommand;

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

    public TestServiceProvider(
        bool includeEventPublisher = true,
        bool includeCommandSender = true)
    {
        if (includeEventPublisher)
        {
            Add<IEventPublisher>(EventPublisher);
        }

        if (includeCommandSender)
        {
            Add<ICommandSender>(CommandSender);
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

internal sealed class ScopedDependency;
