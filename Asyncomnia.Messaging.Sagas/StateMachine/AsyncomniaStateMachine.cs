using Asyncomnia.Messaging.Sagas.StateMachine.Behavior;
using Asyncomnia.Messaging.Sagas.StateMachine.Configuration;

namespace Asyncomnia.Messaging.Sagas.StateMachine;

public abstract class AsyncomniaStateMachine<TSagaInstance> where TSagaInstance : AsyncomniaSagaInstance
{
    private readonly Dictionary<string, State> _states = new();
    private readonly Dictionary<Type, ISagaEvent> _events = new();
    private readonly Dictionary<BehaviorKey, SagaBehaviorConfiguration<TSagaInstance>> _behaviors = new();

    public IReadOnlyList<State> States => _states.Values.ToArray();
    public IReadOnlyList<Type> MessageTypes => _events.Keys.ToArray();

    protected AsyncomniaStateMachine()
    {
        RegisterState(Configuration.State.Initial);
        RegisterState(Configuration.State.Final);
    }

    protected State State(string name)
    {
        var state = new State(name);
        RegisterState(state);
        return state;
    }

    protected SagaEvent<TMessage> Event<TMessage>(Action<SagaEventConfiguration<TMessage>> configuration) where TMessage: class, IMessage
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var eventType = typeof(TMessage);

        if (_events.ContainsKey(eventType))
        {
            throw new InvalidOperationException(
                $"State machine '{GetType().FullName}' already contains an event for message type '{eventType.FullName}'.");
        }

        var sagaEventConfiguration = new SagaEventConfiguration<TMessage>();
        configuration(sagaEventConfiguration);

        var sagaEvent = sagaEventConfiguration.Build();

        _events[eventType] = sagaEvent;

        return sagaEvent;
    }

    protected void Initially(params SagaBehaviorConfiguration<TSagaInstance>[]  behaviors)
    {
       RegisterBehaviors(Configuration.State.Initial, behaviors);
    }

    protected void During(State state, params SagaBehaviorConfiguration<TSagaInstance>[] behaviors)
    {
        ArgumentNullException.ThrowIfNull(state);

        if (!_states.ContainsKey(state.Name))
        {
            throw new InvalidOperationException(
                $"State '{state.Name}' is not registered in state machine '{GetType().FullName}'. Register the state before configuring its behaviors.");
        }

        RegisterBehaviors(state, behaviors);
    }

    protected SagaBehaviorConfiguration<TSagaInstance, TMessage> When<TMessage>(SagaEvent<TMessage> @event) where TMessage : class, IMessage
    {
        ArgumentNullException.ThrowIfNull(@event);

        if (!_events.ContainsKey(typeof(TMessage)))
        {
            throw new InvalidOperationException(
                $"Event for message type '{typeof(TMessage).FullName}' is not registered in state machine '{GetType().FullName}'.");
        }

        if (!ReferenceEquals(@event, _events[typeof(TMessage)]))
        {
            throw new InvalidOperationException(
                $"The provided event for message type '{typeof(TMessage).FullName}' does not belong to state machine '{GetType().FullName}'.");
        }

        return new SagaBehaviorConfiguration<TSagaInstance, TMessage>(@event, States);
    }

    internal bool TryGetBehavior(string currentState, Type messageType, out SagaBehaviorConfiguration<TSagaInstance>? behavior)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(currentState);
        ArgumentNullException.ThrowIfNull(messageType);

        var behaviorKey = new BehaviorKey(currentState, messageType);

        return _behaviors.TryGetValue(behaviorKey, out behavior);
    }

    internal bool TryGetEvent(Type messageType, out ISagaEvent? sagaEvent)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        return _events.TryGetValue(messageType, out sagaEvent);
    }

    private void RegisterState(State state)
    {
        if (_states.TryGetValue(state.Name, out var existingState) && existingState is not null)
        {
            throw new InvalidOperationException(
                $"State machine '{GetType().FullName}' already contains a state named '{state.Name}'.");
        }

        _states[state.Name] = state;
    }

    private void RegisterBehaviors(State state, IEnumerable<SagaBehaviorConfiguration<TSagaInstance>> definitions)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(definitions);

        if (definitions.Any(x => x is null))
        {
            throw new ArgumentException($"Behavior collection for state '{state.Name}' cannot contain null elements", nameof(definitions));
        }

        foreach (var definition in definitions)
        {
            // Tie key (state + event) to list of definied activities.
            // So we can have key for Initial + Event1, Initial + Event2 etc.
            var key = new BehaviorKey(state.Name, definition.MessageType);

            if (!_behaviors.TryAdd(key, definition))
            {
                throw new InvalidOperationException(
                    $"State machine '{GetType().FullName}' already contains a behavior for message type '{definition.MessageType.FullName}' in state '{state.Name}'.");
            }
        }
    }
}
