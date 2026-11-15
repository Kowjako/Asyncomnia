using Asyncomnia.Messaging.Sagas.StateMachine.Configuration;

namespace Asyncomnia.Messaging.Sagas.StateMachine.Behavior;

public abstract class SagaBehaviorConfiguration<TSagaInstance> where TSagaInstance : AsyncomniaSagaInstance
{
    public abstract Type MessageType { get; }

    internal abstract ValueTask ExecuteAsync(TSagaInstance body, object message, CancellationToken token);
}

public class SagaBehaviorConfiguration<TSagaInstance, TMessage> : SagaBehaviorConfiguration<TSagaInstance> 
    where TSagaInstance : AsyncomniaSagaInstance
    where TMessage: class, IMessage
{
    private readonly List<Func<SagaContext<TSagaInstance, TMessage>, ValueTask>> _activities = [];
    internal IReadOnlyList<Func<SagaContext<TSagaInstance, TMessage>, ValueTask>> Activities => _activities;

    public override Type MessageType => typeof(TMessage);

    private readonly SagaEvent<TMessage> _sagaEvent;
    private readonly IReadOnlyList<State> _registeredStates;

    internal SagaBehaviorConfiguration(SagaEvent<TMessage> sagaEvent, IReadOnlyList<State> registeredStates)
    {
        ArgumentNullException.ThrowIfNull(sagaEvent);
        ArgumentNullException.ThrowIfNull(registeredStates);

        _sagaEvent = sagaEvent;
        _registeredStates = registeredStates;
    }

    public SagaBehaviorConfiguration<TSagaInstance, TMessage> Then(Action<SagaContext<TSagaInstance, TMessage>> action)
    {
        ArgumentNullException.ThrowIfNull(action);

        _activities.Add(ctx =>
        {
            ctx.CancellationToken.ThrowIfCancellationRequested();
            action(ctx);
            return ValueTask.CompletedTask;
        });

        return this;
    }

    public SagaBehaviorConfiguration<TSagaInstance, TMessage> ThenAsync(Func<SagaContext<TSagaInstance, TMessage>, Task> action)
    {
        ArgumentNullException.ThrowIfNull(action);

        _activities.Add(async (ctx) =>
        {
            ctx.CancellationToken.ThrowIfCancellationRequested();
            await action(ctx);
        });

        return this;
    }

    public SagaBehaviorConfiguration<TSagaInstance, TMessage> If(
        Func<SagaContext<TSagaInstance, TMessage>, bool> predicate,
        Action<SagaBehaviorConfiguration<TSagaInstance, TMessage>> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        ArgumentNullException.ThrowIfNull(predicate);

        var actionBuilder = new SagaBehaviorConfiguration<TSagaInstance, TMessage>(_sagaEvent, _registeredStates);
        action(actionBuilder);

        _activities.Add(async ctx =>
        {
            ctx.CancellationToken.ThrowIfCancellationRequested();
            if (predicate(ctx))
            {
                foreach (var action in actionBuilder.Activities)
                {
                    ctx.CancellationToken.ThrowIfCancellationRequested();
                    await action(ctx);
                }
            }
        });

        return this;
    }

    public SagaBehaviorConfiguration<TSagaInstance, TMessage> IfAsync(
         Func<SagaContext<TSagaInstance, TMessage>, Task<bool>> predicate,
         Action<SagaBehaviorConfiguration<TSagaInstance, TMessage>> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        ArgumentNullException.ThrowIfNull(predicate);

        var actionBuilder = new SagaBehaviorConfiguration<TSagaInstance, TMessage>(_sagaEvent, _registeredStates);
        action(actionBuilder);

        _activities.Add(async ctx =>
        {
            ctx.CancellationToken.ThrowIfCancellationRequested();
            if (await predicate(ctx))
            {
                foreach (var action in actionBuilder.Activities)
                {
                    ctx.CancellationToken.ThrowIfCancellationRequested();
                    await action(ctx);
                }
            }
        });

        return this;
    }

    public SagaBehaviorConfiguration<TSagaInstance, TMessage> IfElse(
        Predicate<SagaContext<TSagaInstance, TMessage>> predicate,
        Action<SagaBehaviorConfiguration<TSagaInstance, TMessage>> ifAction,
        Action<SagaBehaviorConfiguration<TSagaInstance, TMessage>> elseAction)
    {
        ArgumentNullException.ThrowIfNull(ifAction);
        ArgumentNullException.ThrowIfNull(elseAction);
        ArgumentNullException.ThrowIfNull(predicate);

        var ifActionBuilder = new SagaBehaviorConfiguration<TSagaInstance, TMessage>(_sagaEvent, _registeredStates);
        ifAction(ifActionBuilder);

        var elseActionBuilder = new SagaBehaviorConfiguration<TSagaInstance, TMessage>(_sagaEvent, _registeredStates);
        elseAction(elseActionBuilder);

        _activities.Add(async ctx =>
        {
            ctx.CancellationToken.ThrowIfCancellationRequested();
            if (predicate(ctx))
            {
                foreach (var action in ifActionBuilder.Activities)
                {
                    ctx.CancellationToken.ThrowIfCancellationRequested();
                    await action(ctx);
                }
            } 
            else
            {
                foreach (var action in elseActionBuilder.Activities)
                {
                    ctx.CancellationToken.ThrowIfCancellationRequested();
                    await action(ctx);
                }
            }
        });

        return this;
    }

    public SagaBehaviorConfiguration<TSagaInstance, TMessage> IfElseAsync(
        Func<SagaContext<TSagaInstance, TMessage>, Task<bool>> predicate,
        Action<SagaBehaviorConfiguration<TSagaInstance, TMessage>> ifAction,
        Action<SagaBehaviorConfiguration<TSagaInstance, TMessage>> elseAction)
    {
        ArgumentNullException.ThrowIfNull(ifAction);
        ArgumentNullException.ThrowIfNull(elseAction);
        ArgumentNullException.ThrowIfNull(predicate);

        var ifActionBuilder = new SagaBehaviorConfiguration<TSagaInstance, TMessage>(_sagaEvent, _registeredStates);
        ifAction(ifActionBuilder);

        var elseActionBuilder = new SagaBehaviorConfiguration<TSagaInstance, TMessage>(_sagaEvent, _registeredStates);
        elseAction(elseActionBuilder);

        _activities.Add(async ctx =>
        {
            ctx.CancellationToken.ThrowIfCancellationRequested();
            if (await predicate(ctx))
            {
                foreach (var action in ifActionBuilder.Activities)
                {
                    ctx.CancellationToken.ThrowIfCancellationRequested();
                    await action(ctx);
                }
            }
            else
            {
                foreach (var action in elseActionBuilder.Activities)
                {
                    ctx.CancellationToken.ThrowIfCancellationRequested();
                    await action(ctx);
                }
            }
        });

        return this;
    }

    public SagaBehaviorConfiguration<TSagaInstance, TMessage> TransitionTo(State state)
    {
        ArgumentNullException.ThrowIfNull(state);

        if (!_registeredStates.Contains(state))
        {
            throw new InvalidOperationException(
                $"Cannot transition to state '{state.Name}' because it is not registered in the state machine. Register states before configuring transitions.");
        }

        _activities.Add(ctx =>
        {
            ctx.CancellationToken.ThrowIfCancellationRequested();
            ctx.State.CurrentState = state.Name;
            return ValueTask.CompletedTask;
        });

        return this;
    }

    public SagaBehaviorConfiguration<TSagaInstance, TMessage> Finalize()
    {
        _activities.Add(ctx =>
        {
            ctx.CancellationToken.ThrowIfCancellationRequested();
            ctx.State.CurrentState = State.Final.Name;
            return ValueTask.CompletedTask;
        });

        return this;
    }

    internal override async ValueTask ExecuteAsync(TSagaInstance body, object message, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(body);
        ArgumentNullException.ThrowIfNull(message);

        if (message is not TMessage typedMessage)
        {
            throw new ArgumentException(
                $"Behavior for message type '{typeof(TMessage).FullName}' cannot execute a message of type '{message.GetType().FullName}'.", nameof(message));
        }

        var sagaContext = new SagaContext<TSagaInstance, TMessage>
        {
            Message = typedMessage,
            State = body,
            CancellationToken = token
        };

        foreach (var activity in _activities)
        {
            token.ThrowIfCancellationRequested();
            await activity(sagaContext);
        }
    }
}