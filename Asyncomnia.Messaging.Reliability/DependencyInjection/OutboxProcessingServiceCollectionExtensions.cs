using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Asyncomnia.Messaging.Reliability.Outbox.Processing;
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
        services.AddOptions<OutboxOptions>().ValidateOnStart();

        services.TryAddScoped<OutboxBatchProcessor>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, OutboxBackgroundJob>());

        return services;
    }
}
