using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

internal sealed class RabbitMqConsumerOptionsValidator : IValidateOptions<RabbitMqConsumerOptions>
{
    public ValidateOptionsResult Validate(string? name, RabbitMqConsumerOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.EndpointName))
        {
            return ValidateOptionsResult.Fail("RabbitMqConsumerOptions:EndpointName cannot be empty");
        }

        return ValidateOptionsResult.Success;
    }
}