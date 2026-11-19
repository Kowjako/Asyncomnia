using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Outbox.Configuration;

internal sealed class OutboxStorageOptionsValidator(IServiceProviderIsService serviceProviderIsService) : IValidateOptions<OutboxOptions>
{
    public ValidateOptionsResult Validate(string? name, OutboxOptions options)
    {
        if (!serviceProviderIsService.IsService(typeof(IOutboxStore)))
        {
            return ValidateOptionsResult.Fail("Outbox storage is not configured. Register a storage provider, for example by calling UseSqlServer(...).");
        }

        if (options.CleanupEnabled && !serviceProviderIsService.IsService(typeof(IOutboxCleanupStore)))
        {
            return ValidateOptionsResult.Fail("Outbox cleanup is enabled, but the configured storage provider does not support it. Register an IOutboxCleanupStore implementation or disable outbox cleanup.");
        }

        return ValidateOptionsResult.Success;
    }
}
