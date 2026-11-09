using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

internal sealed class RabbitMqOptionsValidator : IValidateOptions<RabbitMqOptions>
{
    public ValidateOptionsResult Validate(string? name, RabbitMqOptions options)
    {
        if (!Uri.TryCreate(options.ConnectionString, UriKind.Absolute, out var connectionUri))
        {
            return ValidateOptionsResult.Fail("RabbitMqOptions:ConnectionString must be a valid absolute URI");
        }

        if (!connectionUri.Scheme.Equals("amqp", StringComparison.OrdinalIgnoreCase) &&
            !connectionUri.Scheme.Equals("amqps", StringComparison.OrdinalIgnoreCase))
        {
            return ValidateOptionsResult.Fail("RabbitMqOptions:ConnectionString must use the amqp or amqps scheme");
        }

        return ValidateOptionsResult.Success;
    }
}