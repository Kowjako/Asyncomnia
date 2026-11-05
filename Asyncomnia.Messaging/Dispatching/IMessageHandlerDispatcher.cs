namespace Asyncomnia.Messaging.Dispatching;

public interface IMessageHandlerDispatcher
{
    Task DispatchEventAsync<TEvent>(MessageEnvelope<TEvent> envelope, CancellationToken ct = default)
        where TEvent : class, IEvent;

    Task DispatchCommandAsync<TCommand>(MessageEnvelope<TCommand> envelope, CancellationToken ct = default)
        where TCommand : class, ICommand;
}