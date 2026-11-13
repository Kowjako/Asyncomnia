using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.DelayedPublishing.Configuration;

internal sealed class DelayedPublishingOptionsValidator : IValidateOptions<DelayedPublishingOptions>
{
    public ValidateOptionsResult Validate(string? name, DelayedPublishingOptions options)
    {
        if (options.BatchSize <= 0)
        {
            return ValidateOptionsResult.Fail("DelayedPublishingOptions:BatchSize must be greater than zero");
        }

        if (options.PollingInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("DelayedPublishingOptions:PollingInterval must be greater than zero");
        }

        return ValidateOptionsResult.Success;
    }
}