using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.Sagas.StateMachine;

namespace Asyncomnia.Messaging.Sagas.Runtime;

// SagaEventHandler is used to handle events by common Asyncomnia MessageHandlerDispatcher,
// because when event arrives, MessageHandlerDispatcher is looking for handlers, here
// our handler just runs saga dedicated for this event :D
internal sealed class SagaEventHandler<TStateMachine, TSagaInstance, TEvent> : IEventHandler<TEvent> 
    where TEvent : class, IEvent
    where TStateMachine: AsyncomniaStateMachine<TSagaInstance>
    where TSagaInstance: AsyncomniaSagaInstance, new()
{
    private readonly SagaRuntime<TStateMachine, TSagaInstance> _runtime;

    public SagaEventHandler(SagaRuntime<TStateMachine, TSagaInstance> runtime)
    {
        _runtime = runtime ?? throw new ArgumentNullException(nameof(runtime));
    }

    public Task HandleAsync(TEvent message, MessageMetadata metadata, CancellationToken ct)
    {
        return _runtime.HandleAsync(message, metadata, ct);
    }
}
