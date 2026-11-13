using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.Inbox.Configuration;
using Asyncomnia.Messaging.Reliability.Inbox.Processing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.DependencyInjection;

public static class InboxBuilderExtensions
{
    public static AsyncomniaMessagingBuilder UseInbox(this AsyncomniaMessagingBuilder builder, Action<InboxBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Cleanup IInboundMessagePipeline registration which comes from AddAsyncomniaMessaging()
        builder.Services.RemoveAll<IInboundMessagePipeline>();
        builder.Services.TryAddScoped<HandlerMessagePipeline>();
        builder.Services.AddScoped<IInboundMessagePipeline, InboxMessagePipeline>();

        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<InboxOptions>, InboxOptionsValidator>());
        builder.Services.AddOptions<InboxOptions>().ValidateOnStart();

        builder.Services.TryAddScoped<InboxCleanupProcessor>();
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, InboxCleanupBackgroundJob>());

        var inboxBuilder = new InboxBuilder(builder.Services);
        configure?.Invoke(inboxBuilder);

        return builder;
    }
}