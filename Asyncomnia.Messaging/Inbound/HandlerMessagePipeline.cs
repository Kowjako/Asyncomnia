
using Asyncomnia.Messaging.Dispatching;

namespace Asyncomnia.Messaging.Inbound;

internal sealed class HandlerMessagePipeline(IMessageHandlerDispatcher dispatcher) : IInboundMessagePipeline
{
    public Task HandleCommandAsync<TCommand>(MessageEnvelope<TCommand> envelope, CancellationToken ct = default)
        where TCommand : class, ICommand
    {
        ArgumentNullException.ThrowIfNull(envelope);
        return dispatcher.DispatchCommandAsync(envelope, ct);
    }

    public Task HandleEventAsync<TEvent>(MessageEnvelope<TEvent> envelope, CancellationToken ct = default)
        where TEvent : class, IEvent
    {
        ArgumentNullException.ThrowIfNull(envelope);
        return dispatcher.DispatchEventAsync(envelope, ct);
    }
}
