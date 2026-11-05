namespace Asyncomnia.Messaging.Tests.TestDoubles;

internal sealed record TestEvent(string Value) : IEvent;

internal sealed record TestCommand(string Value) : ICommand;

internal static class TestEnvelopeFactory
{
    public static MessageEnvelope<TMessage> Create<TMessage>(TMessage message) where TMessage : class, IMessage
    {
        var metadata = new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow);

        return new MessageEnvelope<TMessage>(message, metadata);
    }
}
