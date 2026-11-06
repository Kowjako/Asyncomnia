namespace Asyncomnia.Messaging.Transport;

public interface IOutboundTransport
{
    Task DispatchAsync(IMessage message, MessageMetadata metadata, CancellationToken token = default);
}