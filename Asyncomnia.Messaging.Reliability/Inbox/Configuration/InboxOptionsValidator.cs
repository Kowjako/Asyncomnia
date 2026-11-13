using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Inbox.Configuration;

internal sealed class InboxOptionsValidator : IValidateOptions<InboxOptions>
{
    public ValidateOptionsResult Validate(string? name, InboxOptions options)
    {
        if (options.DuplicateDetectionWindow <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("InboxOptions:DuplicateDetectionWindow must be greater than zero");
        }

        if (options.CleanupBatchSize <= 0)
        {
            return ValidateOptionsResult.Fail("InboxOptions:CleanupBatchSize must be greater than zero");
        }

        if (options.CleanupInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("InboxOptions:CleanupInterval must be greater than zero");
        }

        return ValidateOptionsResult.Success;
    }
}