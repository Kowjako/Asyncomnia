using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Outbox.Configuration;

internal sealed class OutboxOptionsValidator : IValidateOptions<OutboxOptions>
{
    public ValidateOptionsResult Validate(string? name, OutboxOptions options)
    {
        if (options.WorkerCount <= 0)
        {
            return ValidateOptionsResult.Fail("OutboxOptions:WorkerCount must be greater than zero");
        }

        if (options.BatchSize <= 0)
        {
            return ValidateOptionsResult.Fail("OutboxOptions:BatchSize must be greater than zero");
        }

        if (options.PollingInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("OutboxOptions:PollingInterval must be greater than zero");
        }

        if (options.LeaseDuration <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("OutboxOptions:LeaseDuration must be greater than zero");
        }

        if (options.MaxAttempts <= 0)
        {
            return ValidateOptionsResult.Fail("OutboxOptions:MaxAttempts must be greater than zero");
        }

        if (options.RetryDelay <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("OutboxOptions:RetryDelay must be greater than zero");
        }

        if (options.ProcessedMessageRetention <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("OutboxOptions:ProcessedMessageRetention must be greater than zero");
        }

        if (options.FailedMessageRetention != null && options.FailedMessageRetention <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("OutboxOptions:FailedMessageRetention must be greater than zero when configured");
        }

        if (options.CleanupBatchSize <= 0)
        {
            return ValidateOptionsResult.Fail("OutboxOptions:CleanupBatchSize must be greater than zero");
        }

        if (options.CleanupInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("OutboxOptions:CleanupInterval must be greater than zero");
        }

        return ValidateOptionsResult.Success;
    }
}
