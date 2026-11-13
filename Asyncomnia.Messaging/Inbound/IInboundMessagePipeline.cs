namespace Asyncomnia.Messaging.Inbound;

public interface IInboundMessagePipeline
{
    Task HandleEventAsync<TEvent>(InboundMessageContext context, CancellationToken token = default)
        where TEvent : class, IEvent;

    Task HandleCommandAsync<TCommand>(InboundMessageContext context, CancellationToken token = default)
        where TCommand : class, ICommand;
}