using Asyncomnia.Messaging.Handlers;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Dispatching;

internal sealed class MessageHandlerDispatcher(IServiceProvider serviceProvider) : IMessageHandlerDispatcher
{
    public async Task DispatchEventAsync<TEvent>(MessageEnvelope<TEvent> envelope, CancellationToken ct = default)
        where TEvent: class, IEvent
    {
        ArgumentNullException.ThrowIfNull(envelope, nameof(envelope));

        var handlers = serviceProvider.GetServices<IEventHandler<TEvent>>().ToArray();

        if (handlers.Length == 0)
        {
            throw new InvalidOperationException($"No event handler is registered for '{typeof(TEvent).FullName}'");
        }

        foreach (var handler in handlers)
        {
            ct.ThrowIfCancellationRequested();

            await handler.HandleAsync(envelope.Message, envelope.Metadata, ct);
        }
    }

    public async Task DispatchCommandAsync<TCommand>(MessageEnvelope<TCommand> envelope, CancellationToken ct = default)
        where TCommand: class, ICommand
    {
        ArgumentNullException.ThrowIfNull(envelope, nameof(envelope));

        var handlers = serviceProvider.GetServices<ICommandHandler<TCommand>>().ToArray();

        if (handlers.Length == 0)
        {
            throw new InvalidOperationException($"No command handlers is registered for '{typeof(TCommand).FullName}'");
        }

        if (handlers.Length > 1)
        {
            throw new InvalidOperationException($"More than one command handler is registered for '{typeof(TCommand).FullName}'");
        }

        await handlers[0].HandleAsync(envelope.Message, envelope.Metadata, ct);
    }
}