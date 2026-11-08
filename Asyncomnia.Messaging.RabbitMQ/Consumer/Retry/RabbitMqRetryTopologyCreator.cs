using Asyncomnia.Messaging.RabbitMQ.Configuration;
using EasyNetQ;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;

internal interface IRabbitMqRetryTopologyCreator
{
    Task DeclareAsync(CancellationToken token = default);
}

internal sealed class RabbitMqRetryTopologyCreator(IAdvancedBus advancedBus,
    IExchangeDeclareStrategy exchangeDeclareStrategy,
    IOptions<RabbitMqConsumerOptions> options,
    RabbitMqSubscriptionResolver subscriptions) : IRabbitMqRetryTopologyCreator
{
    private const string ReturnExchange = "asyncomnia/retry-return";
    private const string Prefix = "asyncomnia/";

    private readonly IAdvancedBus _advancedBus = advancedBus
        ?? throw new ArgumentNullException(nameof(advancedBus));

    private readonly IExchangeDeclareStrategy _exchangeStrategy = exchangeDeclareStrategy
        ?? throw new ArgumentNullException(nameof(exchangeDeclareStrategy));

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

        var delays = _options.BrokerRetryDelays.Distinct();

        var consumerQueues = _subscriptions.Resolve().Select(t => t.QueueName).ToList();

        var returnExchange = await _exchangeStrategy.DeclareExchangeAsync(ReturnExchange, ExchangeType.Direct, token);

        foreach (var delay in delays)
        {
            var delayMilliseconds = checked((long)delay.TotalMilliseconds);

            var exchange = await _exchangeStrategy.DeclareExchangeAsync($"{Prefix}retry-{delayMilliseconds}ms", ExchangeType.Fanout, token);

            var queue = await _advancedBus.QueueDeclareAsync(
                $"{Prefix}retry-{delayMilliseconds}ms-queue",
                configuration =>
                {
                    configuration.AsDurable(true);
                    configuration.AsAutoDelete(false);
                    configuration.AsExclusive(false);
                    configuration.WithMessageTtl(delay);
                    configuration.WithDeadLetterExchange(returnExchange);
                }, token);

            await _advancedBus.QueueBindAsync(queue.Name, exchange.Name, string.Empty, cancellationToken: token);
        }

        foreach (var consumerQueue in consumerQueues)
        {
            await _advancedBus.QueueBindAsync(consumerQueue, returnExchange.Name, consumerQueue, cancellationToken: token);
        }
    }
}
