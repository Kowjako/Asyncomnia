using Asyncomnia.Messaging.Inbound;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Handlers;

internal static class HandlerRegistrationMetadataExtensions
{
    public static void TryAddMessageHandlerType(this IServiceCollection services, Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        if (services.Any(x => x.ServiceType == typeof(MessageHandlerRegistration) &&
                              x.ImplementationInstance is MessageHandlerRegistration r &&
                              r.MessageType == messageType))
        {
            return;
        }

        services.AddSingleton(new MessageHandlerRegistration(messageType));
    }

    public static void TryAddMessageDispatchDescriptor(this IServiceCollection services, MessageDispatchDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);

        if (services.Any(x => x.ServiceType == typeof(MessageDispatchDescriptor) &&
                              x.ImplementationInstance is MessageDispatchDescriptor r &&
                              r.MessageType == descriptor.MessageType))
        {
            return;
        }

        services.AddSingleton(descriptor);
    }
}