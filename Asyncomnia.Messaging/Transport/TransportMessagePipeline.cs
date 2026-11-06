using Asyncomnia.Messaging.Outbound;

namespace Asyncomnia.Messaging.Transport;

internal sealed class TransportMessagePipeline(IOutboundTransport transport) : IOutboundMessagePipeline
{
    public Task PublishAsync<TEvent>(MessageEnvelope<TEvent> envelope, CancellationToken cancellationToken = default)
       where TEvent : class, IEvent
    {
        return transport.DispatchAsync(envelope.Message, envelope.Metadata, cancellationToken);
    }

    public Task SendAsync<TCommand>(MessageEnvelope<TCommand> envelope, CancellationToken cancellationToken = default)
       where TCommand : class, ICommand
    {
        return transport.DispatchAsync(envelope.Message, envelope.Metadata, cancellationToken);
    }
}