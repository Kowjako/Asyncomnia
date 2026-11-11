using System.Text;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;

internal interface IRabbitMqDelayedRetryPublisher
{
    Task<bool> TryPublishAsync(RabbitMqConsumeContext context, CancellationToken token = default);
}

internal sealed class RabbitMqDelayedRetryPublisher(IRabbitMqPublisher rabbitMqPublisher, IOptions<RabbitMqConsumerOptions> options) : IRabbitMqDelayedRetryPublisher
{
    private const string RetryCountHeader = "x-asyncomnia-broker-retry";
    private const string Prefix = "asyncomnia/";

    private readonly IRabbitMqPublisher _rabbitMqPublisher = rabbitMqPublisher
        ?? throw new ArgumentNullException(nameof(rabbitMqPublisher));

    private readonly RabbitMqConsumerOptions _options = options?.Value
        ?? throw new ArgumentNullException(nameof(options));

    public async Task<bool> TryPublishAsync(RabbitMqConsumeContext context, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!_options.BrokerRetriesEnabled)
        {
            return false;
        }

        var retryCount = GetRetryCount(context.Properties);

        if (retryCount >= _options.BrokerRetryDelays.Count)
        {
            return false;
        }

        var nextRetryCount = retryCount + 1;
        var delay = _options.BrokerRetryDelays[retryCount];
        var delayMilliseconds = checked((long)delay.TotalMilliseconds);

        var properties = WithRetryCount(context.Properties, nextRetryCount);

        await _rabbitMqPublisher.PublishAsync(
            exchange: $"{Prefix}retry-{delayMilliseconds}ms",
            // save source queue name, so after Deadlettering
            // it will route to source queue via retry-return exchange
            routingKey: context.QueueName,
            mandatory: true,
            properties: properties,
            body: context.Body,
            token);

        return true;
    }

    private static int GetRetryCount(IReadOnlyBasicProperties props)
    {
        if (props.Headers is null || !props.Headers.TryGetValue(RetryCountHeader, out var value))
        {
            return 0;
        }

        return value switch
        {
            byte number => number,
            short number when number >= 0 => number,
            int number when number >= 0 => number,
            long number when number is >= 0 and <= int.MaxValue => (int)number,
            byte[] bytes when int.TryParse(Encoding.UTF8.GetString(bytes), out var number) && number >= 0 => number,
            _ => 0
        };
    }

    private static BasicProperties WithRetryCount(IReadOnlyBasicProperties props, int retryCount)
    {
        // Create new headers dictionary to not override received headers from RabbitMqConsumeContext
        var headers = props.Headers is null
            ? new Dictionary<string, object?>()
            : new Dictionary<string, object?>(props.Headers);

        headers[RetryCountHeader] = retryCount;

        return new BasicProperties(props)
        {
            Headers = headers,
            Expiration = null
        };
    }
}
