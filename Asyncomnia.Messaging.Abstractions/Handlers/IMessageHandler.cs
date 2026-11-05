namespace Asyncomnia.Messaging.Handlers;

public interface IMessageHandler<in TMessage> where TMessage: class, IMessage
{
    Task HandleAsync(TMessage message, MessageMetadata metadata, CancellationToken ct);
}