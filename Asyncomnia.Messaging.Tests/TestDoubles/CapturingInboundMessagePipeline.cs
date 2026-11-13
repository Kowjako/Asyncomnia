using Asyncomnia.Messaging.Inbound;

namespace Asyncomnia.Messaging.Tests.TestDoubles;

internal sealed class CapturingInboundMessagePipeline : IInboundMessagePipeline
{
    public Task HandleEventAsync<TEvent>(InboundMessageContext context, CancellationToken token = default)
        where TEvent : class, IEvent
    {
        return Task.CompletedTask;
    }

    public Task HandleCommandAsync<TCommand>(InboundMessageContext context, CancellationToken token = default)
        where TCommand : class, ICommand
    {
        return Task.CompletedTask;
    }
}
