namespace Asyncomnia.Messaging.Outbound;

internal sealed class MessageEnvelopeFactory(TimeProvider timeProvider)
{
    public MessageEnvelope<TMessage> Create<TMessage>(TMessage message, MessageDispatchOptions? options)
        where TMessage: class, IMessage
    {
        ArgumentNullException.ThrowIfNull(message, nameof(message));

        if (options?.CorrelationId == Guid.Empty)
        {
            throw new ArgumentException("Correlation ID in options cannot be empty", nameof(options));
        }

        var metadata = new MessageMetadata(Guid.CreateVersion7(), timeProvider.GetUtcNow())
        {
            CorrelationId = options?.CorrelationId
        };

        return new MessageEnvelope<TMessage>(message, metadata);
    }
}