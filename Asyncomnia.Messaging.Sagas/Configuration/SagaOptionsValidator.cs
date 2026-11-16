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

        return failures.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
    }
}