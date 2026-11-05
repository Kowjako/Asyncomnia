namespace Asyncomnia.Messaging;

public sealed record MessageEnvelope<TMessage> where TMessage: class, IMessage
{
    public TMessage Message { get; }
    public MessageMetadata Metadata { get; }

    public MessageEnvelope(TMessage message, MessageMetadata metadata)
    {
        Message = message ?? throw new ArgumentNullException(nameof(message));
        Metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
    }
}