using Asyncomnia.Messaging.Dispatching;

namespace Asyncomnia.Messaging.Tests.TestDoubles;

internal sealed class CapturingMessageHandlerDispatcher : IMessageHandlerDispatcher
{
    public object? EventEnvelope { get; private set; }

    public object? CommandEnvelope { get; private set; }

    public CancellationToken EventCancellationToken { get; private set; }

    public CancellationToken CommandCancellationToken { get; private set; }

    public Task DispatchEventAsync<TEvent>(MessageEnvelope<TEvent> envelope, CancellationToken ct = default)
        where TEvent : class, IEvent
    {
        EventEnvelope = envelope;
        EventCancellationToken = ct;

        return Task.CompletedTask;
    }

    public Task DispatchCommandAsync<TCommand>(MessageEnvelope<TCommand> envelope, CancellationToken ct = default)
        where TCommand : class, ICommand
    {
        CommandEnvelope = envelope;
        CommandCancellationToken = ct;

        return Task.CompletedTask;
    }
}
