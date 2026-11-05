using Asyncomnia.Messaging.Outbound;

namespace Asyncomnia.Messaging.Tests.TestDoubles;

internal sealed class CapturingOutboundMessagePipeline : IOutboundMessagePipeline
{
    public object? PublishedEnvelope { get; private set; }

    public object? SentEnvelope { get; private set; }

    public CancellationToken PublishedCancellationToken { get; private set; }

    public CancellationToken SentCancellationToken { get; private set; }

    public int PublishCalls { get; private set; }

    public int SendCalls { get; private set; }

    public Task PublishAsync<TEvent>(MessageEnvelope<TEvent> envelope, CancellationToken token = default)
        where TEvent : class, IEvent
    {
        PublishedEnvelope = envelope;
        PublishedCancellationToken = token;
        PublishCalls++;

        return Task.CompletedTask;
    }

    public Task SendAsync<TCommand>(MessageEnvelope<TCommand> envelope, CancellationToken token = default)
        where TCommand : class, ICommand
    {
        SentEnvelope = envelope;
        SentCancellationToken = token;
        SendCalls++;

        return Task.CompletedTask;
    }
}
