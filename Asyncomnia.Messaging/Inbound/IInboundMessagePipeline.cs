namespace Asyncomnia.Messaging.Inbound;

public interface IInboundMessagePipeline
{
    Task HandleEventAsync<TEvent>(MessageEnvelope<TEvent> envelope, CancellationToken token = default)
        where TEvent : class, IEvent;

    Task HandleCommandAsync<TCommand>(MessageEnvelope<TCommand> envelope, CancellationToken token = default)
        where TCommand : class, ICommand;
}