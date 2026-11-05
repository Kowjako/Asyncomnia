namespace Asyncomnia.Messaging.Transport;

public interface IOutboundMessageTransport
{
    Task DispatchAsync(IMessage message, MessageMetadata metadata, CancellationToken token = default);
}