using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Internal.Serialization;
using Asyncomnia.Messaging.Outbound;
using Asyncomnia.Messaging.Retry;
using Asyncomnia.Messaging.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static AsyncomniaMessagingBuilder AddAsyncomniaMessaging(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services, nameof(services));

        services.TryAddScoped<IMessageHandlerDispatcher, MessageHandlerDispatcher>();
        services.TryAddScoped<IInboundMessagePipeline, HandlerMessagePipeline>();
        services.TryAddScoped<IInboundMessageProcessor, InboundMessageProcessor>();

        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<MessageEnvelopeFactory>();

        services.TryAddScoped<IEventPublisher, EventPublisher>();
        services.TryAddScoped<ICommandSender, CommandSender>();

        services.TryAddScoped<IOutboundMessagePipeline, TransportMessagePipeline>();

        // Stores all type of registered Command/Event Handlers, based on that we will
        // create topology (queues and appropriate binding keys)
        services.TryAddSingleton<IMessageHandlerCatalog, MessageHandlerCatalog>();
        
        // Maps evnet/command type to dispatch delegate, used by rabbitmq consumer
        services.TryAddSingleton<IMessageDispatchCatalog, MessageDispatchCatalog>();

        services.AddOptions<InMemoryRetryOptions>();
        services.TryAddSingleton<IInMemoryRetryExecutor, InMemoryRetryExecutor>();

        services.TryAddSingleton<MessageJsonSerializer>();

        return new AsyncomniaMessagingBuilder(services);
    }
}
