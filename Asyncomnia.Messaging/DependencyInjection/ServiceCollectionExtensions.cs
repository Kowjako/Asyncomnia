using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Outbound;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static AsyncomniaMessagingBuilder AddAsyncomniaMessaging(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services, nameof(services));

        services.TryAddSingleton<IMessageHandlerDispatcher, MessageHandlerDispatcher>();
        services.TryAddSingleton<TimeProvider>(TimeProvider.System);
        services.TryAddScoped<IEventPublisher, EventPublisher>();
        services.TryAddScoped<ICommandSender, CommandSender>();

        return new AsyncomniaMessagingBuilder(services);
    }
}
