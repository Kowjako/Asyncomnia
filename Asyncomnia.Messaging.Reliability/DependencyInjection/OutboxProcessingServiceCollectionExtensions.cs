using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Asyncomnia.Messaging.Reliability.Outbox.Processing;
using Asyncomnia.Messaging.Reliability.Outbox.Processing.Retention;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.DependencyInjection;

internal static class OutboxProcessingServiceCollectionExtensions
{
    public static IServiceCollection AddOutboxProcessing(this IServiceCollection services)
    {
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<OutboxOptions>, OutboxOptionsValidator>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<OutboxOptions>, OutboxStorageOptionsValidator>());
        services.AddOptions<OutboxOptions>().ValidateOnStart();

        services.TryAddScoped<OutboxBatchProcessor>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, OutboxBackgroundJob>());

        services.TryAddScoped<OutboxCleanupProcessor>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, OutboxCleanupBackgroundJob>());

        return services;
    }
}
