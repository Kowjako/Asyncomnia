
using Asyncomnia.Messaging.Dispatching;

namespace Asyncomnia.Messaging.Inbound;

internal sealed class HandlerMessagePipeline(IMessageHandlerDispatcher dispatcher) : IInboundMessagePipeline
{
    public Task HandleCommandAsync<TCommand>(InboundMessageContext context, CancellationToken ct = default)
        where TCommand : class, ICommand
    {
        ArgumentNullException.ThrowIfNull(context);

        var envelope = new MessageEnvelope<TCommand>((TCommand)context.Message, context.Metadata);

        return dispatcher.DispatchCommandAsync(envelope, ct);
    }

    public Task HandleEventAsync<TEvent>(InboundMessageContext context, CancellationToken ct = default)
        where TEvent : class, IEvent
    {
        ArgumentNullException.ThrowIfNull(context);

        var envelope = new MessageEnvelope<TEvent>((TEvent)context.Message, context.Metadata);

        return dispatcher.DispatchEventAsync(envelope, ct);
    }
}
