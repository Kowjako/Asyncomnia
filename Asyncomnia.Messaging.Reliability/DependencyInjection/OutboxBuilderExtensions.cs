using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Outbound;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Asyncomnia.Messaging.Reliability.Outbox.Processing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.DependencyInjection;

public static class OutboxBuilderExtensions
{
    public static AsyncomniaMessagingBuilder UseOutbox(this AsyncomniaMessagingBuilder builder, Action<OutboxBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.RemoveAll<IOutboundMessagePipeline>();
        builder.Services.AddScoped<IOutboundMessagePipeline, OutboxMessagePipeline>();

        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<OutboxOptions>, OutboxOptionsValidator>());
        builder.Services.AddOptions<OutboxOptions>().ValidateOnStart();

        builder.Services.TryAddScoped<OutboxBatchProcessor>();
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, OutboxBackgroundJob>());

        var outboxBuilder = new OutboxBuilder(builder.Services);
        configure?.Invoke(outboxBuilder);

        return builder;
    }
}
