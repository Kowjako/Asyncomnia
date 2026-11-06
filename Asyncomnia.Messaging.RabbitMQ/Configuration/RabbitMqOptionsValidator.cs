using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

internal sealed class RabbitMqOptionsValidator : IValidateOptions<RabbitMqOptions>
{
    public ValidateOptionsResult Validate(string? name, RabbitMqOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return ValidateOptionsResult.Fail("RabbitMqOptions:ConnectionString cannot be empty");
        }

        return ValidateOptionsResult.Success;
    }
}