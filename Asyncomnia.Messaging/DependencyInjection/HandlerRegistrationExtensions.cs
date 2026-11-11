using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.Inbound;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.DependencyInjection;

public static class HandlerRegistrationExtensions
{
    public static AsyncomniaMessagingBuilder RegisterEventHandler<TEvent, THandler>(this AsyncomniaMessagingBuilder builder)
        where TEvent: class, IEvent
        where THandler: class, IEventHandler<TEvent>
    {
        ArgumentNullException.ThrowIfNull(builder, nameof(builder));

        // Prevents duplicate registration of the same handler implementation.
        // Multiple different handlers for one event are allowed.
        builder.Services.TryAddEnumerable(ServiceDescriptor.Scoped<IEventHandler<TEvent>, THandler>());

        builder.Services.TryAddMessageHandlerType(typeof(TEvent));
        builder.Services.TryAddMessageDispatchDescriptor(MessageDispatchDescriptorFactory.CreateEvent<TEvent>());
        return builder;
    }

    public static AsyncomniaMessagingBuilder RegisterCommandHandler<TCommand, THandler>(this AsyncomniaMessagingBuilder builder)
        where TCommand : class, ICommand
        where THandler : class, ICommandHandler<TCommand>
    {
        ArgumentNullException.ThrowIfNull(builder, nameof(builder));

        // Prevents duplicate registration of the same implementation.
        // The dispatcher requires exactly one command handler.
        builder.Services.TryAddEnumerable(ServiceDescriptor.Scoped<ICommandHandler<TCommand>, THandler>());

        builder.Services.TryAddMessageHandlerType(typeof(TCommand));
        builder.Services.TryAddMessageDispatchDescriptor(MessageDispatchDescriptorFactory.CreateComamnd<TCommand>());
        return builder;
    }
}