using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.DelayedPublishing.Configuration;
using Asyncomnia.Messaging.Reliability.DelayedPublishing.Processing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.DependencyInjection;

public static class DelayedPublishingBuilderExtensions
{
    public static AsyncomniaMessagingBuilder UseDelayedPublishing(this AsyncomniaMessagingBuilder builder, Action<DelayedPublishingOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.TryAddScoped<IMessageScheduler, MessageScheduler>();

        builder.Services.TryAddScoped<DelayedMessageBatchProcessor>();
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, DelayedMessageBackgroundJob>());

        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<DelayedPublishingOptions>, DelayedPublishingOptionsValidator>());

        var optionsBuilder = builder.Services.AddOptions<DelayedPublishingOptions>();

        if (configure is not null)
        {
            optionsBuilder.Configure(configure);
        }

        optionsBuilder.ValidateOnStart();

        builder.Services.AddOutboxProcessing();

        return builder;
    }
}
