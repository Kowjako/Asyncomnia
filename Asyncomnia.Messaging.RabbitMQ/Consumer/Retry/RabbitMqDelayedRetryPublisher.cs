using System.Text;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using EasyNetQ;
using EasyNetQ.Consumer;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;

internal interface IRabbitMqDelayedRetryPublisher
{
    Task<bool> TryPublishAsync(ConsumeContext context, CancellationToken token = default);
}

internal sealed class RabbitMqDelayedRetryPublisher(IAdvancedBus advancedBus, IOptions<RabbitMqConsumerOptions> options) : IRabbitMqDelayedRetryPublisher
{
    private const string RetryCountHeader = "x-asyncomnia-broker-retry";
    private const string Prefix = "asyncomnia/";

    private readonly IAdvancedBus _advancedBus = advancedBus
        ?? throw new ArgumentNullException(nameof(advancedBus));

    private readonly RabbitMqConsumerOptions _options = options?.Value
        ?? throw new ArgumentNullException(nameof(options));

    public async Task<bool> TryPublishAsync(ConsumeContext context, CancellationToken token = default)
    {
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

        await _advancedBus.PublishAsync(
            exchange: $"{Prefix}retry-{delayMilliseconds}ms",
            // save source queue name, so after Deadlettering
            // it will route to source queue via retry-return exchange
            routingKey: context.ReceivedInfo.Queue,
            mandatory: true,
            publisherConfirms: true,
            properties: properties,
            body: context.Body,
            cancellationToken: token);

        return true;
    }

    private static int GetRetryCount(MessageProperties props)
    {
        if (!props.HeadersPresent || !props.Headers.TryGetValue(RetryCountHeader, out var value))
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

    private static MessageProperties WithRetryCount(MessageProperties props, int retryCount)
    {
        var headers = props.HeadersPresent
            ? new Dictionary<string, object>(props.Headers)
            : new Dictionary<string, object>();

        headers[RetryCountHeader] = retryCount;

        return props with
        {
            Headers = headers,
            Expiration = null
        };
    }
}
