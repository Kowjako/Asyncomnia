using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.Sagas.StateMachine;

namespace Asyncomnia.Messaging.Sagas.Runtime;

// SagaCommandHandler is used to handle commands by common Asyncomnia MessageHandlerDispatcher,
// because when command arrives, MessageHandlerDispatcher is looking for handlers, here
// our handler just runs saga dedicated for this command :D
internal class SagaCommandHandler<TStateMachine, TSagaInstance, TCommand> : ICommandHandler<TCommand>
    where TCommand : class, ICommand
    where TStateMachine : AsyncomniaStateMachine<TSagaInstance>
    where TSagaInstance : AsyncomniaSagaInstance, new()
{
    private readonly SagaRuntime<TStateMachine, TSagaInstance> _runtime;

    public SagaCommandHandler(SagaRuntime<TStateMachine, TSagaInstance> runtime)
    {
        _runtime = runtime ?? throw new ArgumentNullException(nameof(runtime));
    }

    public Task HandleAsync(TCommand message, MessageMetadata metadata, CancellationToken ct)
    {
        return _runtime.HandleAsync(message, metadata, ct);
    }
}
