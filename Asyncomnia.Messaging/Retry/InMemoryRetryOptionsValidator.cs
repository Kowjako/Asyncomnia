using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Retry;

internal sealed class InMemoryRetryOptionsValidator : IValidateOptions<InMemoryRetryOptions>
{
    public ValidateOptionsResult Validate(string? name, InMemoryRetryOptions options)
    {
        return options.IsConfigured
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail("In-memory retry strategy must be configured using Immediately() or Intervals().");
    }
}