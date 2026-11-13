using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Reliability.Inbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

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

        var inboxBuilder = new InboxBuilder(builder.Services);
        configure?.Invoke(inboxBuilder);

        return builder;
    }
}