using Asyncomnia.Messaging.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Sagas.Configuration;

internal sealed class SagaOptionsValidator(IServiceProviderIsService services) : IValidateOptions<SagaOptions>
{
    public ValidateOptionsResult Validate(string? name, SagaOptions options)
    {
        var failures = new List<string>();

        if (!services.IsService(typeof(InboxFeatureRegistration)))
        {
            failures.Add("Asyncomnia sagas require Inbox processing. Call UseInbox() when configuring Asyncomnia Messaging.");
        }

        if (!services.IsService(typeof(OutboxFeatureRegistration)))
        {
            failures.Add("Asyncomnia sagas require Outbox processing. Call UseOutbox() when configuring Asyncomnia Messaging.");
        }

        if (options.FinalizedSagaRetention <= TimeSpan.Zero)
        {
            failures.Add("SagaOptions:FinalizedSagaRetention must be greater than zero");
        }

        if (options.CleanupBatchSize <= 0)
        {
            failures.Add("SagaOptions:CleanupBatchSize must be greater than zero");
        }

        if (options.CleanupInterval <= TimeSpan.Zero)
        {
            failures.Add("SagaOptions:CleanupInterval must be greater than zero");
        }

        return failures.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
    }
}
