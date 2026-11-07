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
}