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

        if (options.DefaultConsumerCount <= 0)
        {
            return ValidateOptionsResult.Fail("RabbitMqConsumerOptions:DefaultConsumerCount must be greater than 0");
        }

        if (options.DefaultPrefetchCount == 0)
        {
            return ValidateOptionsResult.Fail("RabbitMqConsumerOptions:DefaultPrefetchCount must be greater than 0");
        }

        if (options.DefaultDispatcherConcurrency == 0)
        {
            return ValidateOptionsResult.Fail("RabbitMqConsumerOptions:DefaultDispatcherConcurrency must be greater than 0");
        }

        foreach (var (exchangeName, subscription) in options.Subscriptions)
        {
            if (subscription.ConsumerCount is <= 0)
            {
                return ValidateOptionsResult.Fail(
                    $"RabbitMqConsumerOptions subscription '{exchangeName}':" +
                    $"{nameof(subscription.ConsumerCount)} must be greater than 0");
            }

            if (subscription.PrefetchCount is 0)
            {
                return ValidateOptionsResult.Fail(
                    $"RabbitMqConsumerOptions subscription '{exchangeName}':" +
                    $"{nameof(subscription.PrefetchCount)} must be greater than 0");
            }

            if (subscription.DispatcherConcurrency is 0)
            {
                return ValidateOptionsResult.Fail(
                    $"RabbitMqConsumerOptions subscription '{exchangeName}':" +
                    $"{nameof(subscription.DispatcherConcurrency)} must be greater than 0");
            }
        }

        return ValidateOptionsResult.Success;
    }
}