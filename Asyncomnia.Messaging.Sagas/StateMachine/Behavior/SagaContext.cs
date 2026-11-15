namespace Asyncomnia.Messaging.Sagas.StateMachine.Behavior;

public sealed class SagaContext<TSagaInstance, TMessage> where TSagaInstance : AsyncomniaSagaInstance
                                                         where TMessage: class, IMessage
{
    public TMessage Message { get; init; } = null!;
    public TSagaInstance State { get; init; } = null!;
    public CancellationToken CancellationToken { get; init; }
}