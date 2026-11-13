using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Outbound;
using Asyncomnia.Messaging.Reliability.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.Reliability.DependencyInjection;

public static class OutboxBuilderExtensions
{
    public static AsyncomniaMessagingBuilder UseOutbox(this AsyncomniaMessagingBuilder builder, Action<OutboxBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.RemoveAll<IOutboundMessagePipeline>();
        builder.Services.AddScoped<IOutboundMessagePipeline, OutboxMessagePipeline>();
        builder.Services.AddOutboxProcessing();

        var outboxBuilder = new OutboxBuilder(builder.Services);
        configure?.Invoke(outboxBuilder);

        return builder;
    }
}
