using Asyncomnia.Messaging.Transport;

namespace Asyncomnia.Messaging.Tests.TestDoubles;

internal sealed class CapturingOutboundMessageTransport : IOutboundMessageTransport
{
    public IMessage? Message { get; private set; }

    public MessageMetadata? Metadata { get; private set; }

    public CancellationToken CancellationToken { get; private set; }

    public int DispatchCalls { get; private set; }

    public Task DispatchAsync(IMessage message, MessageMetadata metadata, CancellationToken token = default)
    {
        Message = message;
        Metadata = metadata;
        CancellationToken = token;
        DispatchCalls++;

        return Task.CompletedTask;
    }
}
