namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

public sealed class RabbitMqConsumerOptions
{
    private readonly Dictionary<string, RabbitMqSubscriptionConsumerOptions> _subscriptions 
        = new(StringComparer.Ordinal);

    public string EndpointName { get; set; } = string.Empty;

    /// <summary>
    /// Number of consumers(channel) per queue/subscription (used for competing consumers pattern)
    /// </summary>
    public int DefaultConsumerCount { get; set; } = 1;

    /// <summary>
    /// Number of unacknowledged messages delivered to a single consumer
    /// </summary>
    public ushort DefaultPrefetchCount { get; set; } = 1;

    /// <summary>
    /// Maximum number of messages processed concurrently by a single consumer
    /// </summary>
    public ushort DefaultDispatcherConcurrency { get; set; } = 1;

    public void ConfigureSubscription(string exchangeName, Action<RabbitMqSubscriptionConsumerOptions> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(exchangeName);
        ArgumentNullException.ThrowIfNull(configure);

        if (!_subscriptions.TryGetValue(exchangeName, out var options))
        {
            options = new RabbitMqSubscriptionConsumerOptions();
            _subscriptions.Add(exchangeName, options);
        }

        configure(options);
    }

    internal RabbitMqConsumerSettings GetConsumerSettings(string exchangeName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(exchangeName);

        _subscriptions.TryGetValue(exchangeName, out var configured);

        return new RabbitMqConsumerSettings(
            configured?.ConsumerCount ?? DefaultConsumerCount,
            configured?.PrefetchCount ?? DefaultPrefetchCount,
            configured?.DispatcherConcurrency ??
            DefaultDispatcherConcurrency);
    }

    internal IReadOnlyDictionary<string, RabbitMqSubscriptionConsumerOptions> Subscriptions => _subscriptions;
}