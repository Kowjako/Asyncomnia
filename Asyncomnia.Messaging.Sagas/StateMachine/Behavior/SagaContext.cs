namespace Asyncomnia.Messaging.Sagas.StateMachine.Behavior;

public sealed class SagaContext<TSagaInstance, TMessage> where TSagaInstance : AsyncomniaSagaInstance
                                                         where TMessage: class, IMessage
{
    public TMessage Message { get; }
    public TSagaInstance State { get; }
    public CancellationToken CancellationToken { get; }

    internal SagaContext(TMessage message, TSagaInstance state, CancellationToken cancellationToken)
    {
        Message = message;
        State = state;
        CancellationToken = cancellationToken;
    }
}