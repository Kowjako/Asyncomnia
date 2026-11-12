namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

public sealed class RabbitMqConsumerOptions
{
    private readonly Dictionary<string, RabbitMqSubscriptionOptions> _subscriptions = new(StringComparer.Ordinal);

    public string EndpointName { get; set; } = string.Empty;

    /// <summary>
    /// Determines whether only one consumer across all application instances
    /// can actively receive messages from a subscription queue. This does not
    /// limit concurrent processing within that active consumer.
    /// </summary>
    public bool DefaultSingleActiveConsumer { get; set; }

    /// <summary>
    /// Number of unacknowledged messages delivered to a single consumer
    /// </summary>
    public ushort DefaultPrefetchCount { get; set; } = 1;

    /// <summary>
    /// Maximum number of messages processed concurrently by a single consumer (channel)
    /// </summary>
    public ushort DefaultDispatcherConcurrency { get; set; } = 1;

    /// <summary>
    /// Determines whether failed messages should be published to an
    /// Asyncomnia dead-letter queue. Enabled by default.
    /// </summary>
    public bool DeadLetteringEnabled { get; set; } = true;

    /// <summary>
    /// Determines whether failed messages should be retried through
    /// broker-managed retry queues before dead-lettering.
    /// </summary>
    public bool BrokerRetriesEnabled { get; set; } = false;

    /// <summary>
    /// Ordered delays applied before a failed message is dead-lettered.
    /// </summary>
    public IReadOnlyList<TimeSpan> BrokerRetryDelays { get; set; } =
    [
        TimeSpan.FromSeconds(30),
        TimeSpan.FromMinutes(1),
        TimeSpan.FromMinutes(5)
    ];

    public void ConfigureSubscription(string exchangeName, Action<RabbitMqSubscriptionOptions> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(exchangeName);
        ArgumentNullException.ThrowIfNull(configure);

        if (!_subscriptions.TryGetValue(exchangeName, out var options))
        {
            options = new RabbitMqSubscriptionOptions();
            _subscriptions.Add(exchangeName, options);
        }

        configure(options);
    }

    internal RabbitMqSubscriptionSettings GetSubscriptionSettings(string exchangeName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(exchangeName);

        _subscriptions.TryGetValue(exchangeName, out var configured);

        return new RabbitMqSubscriptionSettings(
            configured?.SingleActiveConsumer ?? DefaultSingleActiveConsumer,
            configured?.PrefetchCount ?? DefaultPrefetchCount,
            configured?.DispatcherConcurrency ??
            DefaultDispatcherConcurrency);
    }

    internal IReadOnlyDictionary<string, RabbitMqSubscriptionOptions> Subscriptions => _subscriptions;
}
