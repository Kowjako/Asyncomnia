using Asyncomnia.Messaging.Internal;
using Asyncomnia.Messaging.Internal.Serialization;
using Asyncomnia.Messaging.Outbound;

namespace Asyncomnia.Messaging.Reliability.Outbox;

internal sealed class OutboxMessagePipeline(MessageJsonSerializer serializer, IOutboxWriter writer) : IOutboundMessagePipeline
{
    public Task PublishAsync<TEvent>(MessageEnvelope<TEvent> envelope, CancellationToken cancellationToken = default)
       where TEvent : class, IEvent
    {
        return WriteAsync(envelope, cancellationToken);
    }

    public Task SendAsync<TCommand>(MessageEnvelope<TCommand> envelope, CancellationToken cancellationToken = default)
        where TCommand: class, ICommand
    {
        return WriteAsync(envelope, cancellationToken);
    }

    private Task WriteAsync<TMessage>(MessageEnvelope<TMessage> envelope, CancellationToken cancellationToken = default)
        where TMessage: class, IMessage
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var messageType = envelope.Message.GetType();
        var contractType = ClrMessageTypeResolver.GetContractType(messageType);
        var messageJson = serializer.SerializeToString(envelope.Message);

        var outboxMessage = new OutboxMessage(envelope.Metadata.MessageId,
            contractType, messageJson, envelope.Metadata.CorrelationId, envelope.Metadata.CreatedAt);

        return writer.WriteAsync(outboxMessage, cancellationToken);
    }
}