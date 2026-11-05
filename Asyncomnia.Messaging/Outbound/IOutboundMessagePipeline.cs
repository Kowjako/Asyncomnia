namespace Asyncomnia.Messaging.Outbound;

public interface IOutboundMessagePipeline
{
    Task PublishAsync<TEvent>(MessageEnvelope<TEvent> envelope, CancellationToken token = default)
        where TEvent : class, IEvent;

    Task SendAsync<TCommand>(MessageEnvelope<TCommand> envelope, CancellationToken token = default)
        where TCommand : class, ICommand;
}