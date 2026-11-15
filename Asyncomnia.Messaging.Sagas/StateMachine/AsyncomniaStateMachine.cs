using Asyncomnia.Messaging.Sagas.StateMachine.Behavior;
using Asyncomnia.Messaging.Sagas.StateMachine.Configuration;

namespace Asyncomnia.Messaging.Sagas.StateMachine;

public abstract class AsyncomniaStateMachine<TSagaInstance> where TSagaInstance : AsyncomniaSagaInstance
{
    private readonly Dictionary<string, State> _states = new();
    private readonly Dictionary<Type, ISagaEvent> _events = new();
    private readonly Dictionary<BehaviorKey, SagaBehaviorConfiguration<TSagaInstance>> _behaviors = new();

    public IReadOnlyList<State> States => _states.Values.ToArray();
    public IReadOnlyList<ISagaEvent> Events => _events.Values.ToArray();

    public AsyncomniaStateMachine()
    {
        RegisterState(Configuration.State.Initial);
        RegisterState(Configuration.State.Final);
    }

    public State State(string name)
    {
        var state = new State(name);
        RegisterState(state);
        return state;
    }

    public SagaEvent<TMessage> Event<TMessage>(Action<SagaEventConfiguration<TMessage>> configuration) where TMessage: class, IMessage
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var eventType = typeof(TMessage);

        if (_events.ContainsKey(eventType))
        {
            throw new InvalidOperationException("Event already registered");
        }

        var sagaEventConfiguration = new SagaEventConfiguration<TMessage>();
        configuration(sagaEventConfiguration);

        var sagaEvent = sagaEventConfiguration.Build();

        _events[eventType] = sagaEvent;

        return sagaEvent;
    }

    public void Initially(params SagaBehaviorConfiguration<TSagaInstance>[]  behaviors)
    {
       RegisterBehaviors(Configuration.State.Initial, behaviors);
    }

    public void During(State state, params SagaBehaviorConfiguration<TSagaInstance>[] behaviors)
    {
        if (!_states.ContainsKey(state.Name))
        {
            throw new InvalidOperationException();
        }

        RegisterBehaviors(state, behaviors);
    }

    public bool TryGetBehavior(string currentState, object message, out SagaBehaviorConfiguration<TSagaInstance>? behavior)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(currentState);
        ArgumentNullException.ThrowIfNull(message);

        var behaviorKey = new BehaviorKey(new State(currentState), message.GetType());

        if (_behaviors.ContainsKey(behaviorKey))
        {
            behavior = _behaviors[behaviorKey];
            return true;
        }
        else
        {
            behavior = default;
            return false;
        }
    }

    public SagaBehaviorConfiguration<TSagaInstance, TMessage> When<TMessage>(SagaEvent<TMessage> @event) where TMessage: class, IMessage
    {
        if (!_events.ContainsKey(typeof(TMessage)) || !ReferenceEquals(@event, _events[typeof(TMessage)]))
        {
            throw new InvalidOperationException();
        }

        return new SagaBehaviorConfiguration<TSagaInstance, TMessage>(@event, States);
    }

    private void RegisterState(State state)
    {
        if (_states.TryGetValue(state.Name, out var existingState) && existingState is not null)
        {
            throw new InvalidOperationException($"Saga {GetType().Name} already has state '{state.Name}' registered");
        }

        _states[state.Name] = state;
    }

    private void RegisterBehaviors(State state, IEnumerable<SagaBehaviorConfiguration<TSagaInstance>> definitions)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(definitions);

        foreach (var definition in definitions)
        {
            // Tie key (state + event) to list of definied activities.
            // So we can have key for Initial + Event1, Initial + Event2 etc.
            var key = new BehaviorKey(state, definition.MessageType);

            if (!_behaviors.TryAdd(key, definition))
            {
                throw new InvalidOperationException($"Behavior for state '{state.Name}' and message '{definition.MessageType.Name}' is already registered in saga '{GetType().Name}'.");
            }
        }
    }
}
