using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Dispatching;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static AsyncomniaMessagingBuilder AddAsyncomniaMessaging(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services, nameof(services));

        services.TryAddSingleton<IMessageHandlerDispatcher, MessageHandlerDispatcher>();

        return new AsyncomniaMessagingBuilder(services);
    }
}
