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
    public static AsyncomniaMessagingBuilder UseOutbox(this AsyncomniaMessagingBuilder builder, Action<OutboxOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.TryAddSingleton<OutboxJsonSerializer>();

        builder.Services.RemoveAll<IOutboundMessagePipeline>();
        builder.Services.AddScoped<IOutboundMessagePipeline, OutboxMessagePipeline>();

        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<OutboxOptions>, OutboxOptionsValidator>());

        var optionsBuilder = builder.Services.AddOptions<OutboxOptions>().ValidateOnStart();
        if (configure != null)
        {
            optionsBuilder.Configure(configure);
        }

        builder.Services.TryAddScoped<OutboxBatchProcessor>();
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, OutboxBackgroundJob>());

        return builder;
    }
}
