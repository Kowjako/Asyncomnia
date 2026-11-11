using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;

internal interface IRabbitMqRetryTopologyCreator
{
    Task DeclareAsync(CancellationToken token = default);
}

internal sealed class RabbitMqRetryTopologyCreator(IRabbitMqChannelFactory channelFactory,
    IOptions<RabbitMqConsumerOptions> options,
    RabbitMqSubscriptionResolver subscriptions) : IRabbitMqRetryTopologyCreator
{
    private const string ReturnExchange = "asyncomnia/retry-return";
    private const string Prefix = "asyncomnia/";

    private readonly IRabbitMqChannelFactory _channelFactory = channelFactory
        ?? throw new ArgumentNullException(nameof(channelFactory));

    private readonly RabbitMqConsumerOptions _options = options?.Value
        ?? throw new ArgumentNullException(nameof(options));

    private readonly RabbitMqSubscriptionResolver _subscriptions = subscriptions
        ?? throw new ArgumentNullException(nameof(subscriptions));

    public async Task DeclareAsync(CancellationToken token = default)
    {
        if (!_options.BrokerRetriesEnabled)
        {
            return;
        }

        var delays = _options.BrokerRetryDelays.Distinct().ToArray();

        var consumerQueues = _subscriptions.Resolve()
            .Select(t => t.QueueName)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        await using var channel = await _channelFactory.CreateAsync(token: token);

        await channel.ExchangeDeclareAsync(
            exchange: ReturnExchange,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null,
            passive: false,
            noWait: false,
            cancellationToken: token);

        foreach (var delay in delays)
        {
            var delayMilliseconds = checked((long)delay.TotalMilliseconds);
            var exchangeName = $"{Prefix}retry-{delayMilliseconds}ms";
            var queueName = $"{exchangeName}-queue";

            await channel.ExchangeDeclareAsync(
                exchange: exchangeName,
                type: ExchangeType.Fanout,
                durable: true,
                autoDelete: false,
                arguments: null,
                passive: false,
                noWait: false,
                cancellationToken: token);

            var queueArguments = new Dictionary<string, object?>
            {
                ["x-message-ttl"] = delayMilliseconds,
                ["x-dead-letter-exchange"] = ReturnExchange
            };

            await channel.QueueDeclareAsync(
                queue: queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: queueArguments,
                passive: false,
                noWait: false,
                cancellationToken: token);

            await channel.QueueBindAsync(
                queue: queueName,
                exchange: exchangeName,
                routingKey: string.Empty,
                arguments: null,
                noWait: false,
                cancellationToken: token);
        }

        foreach (var consumerQueue in consumerQueues)
        {
            await channel.QueueBindAsync(
                queue: consumerQueue,
                exchange: ReturnExchange,
                routingKey: consumerQueue,
                arguments: null,
                noWait: false,
                cancellationToken: token);
        }
    }
}
