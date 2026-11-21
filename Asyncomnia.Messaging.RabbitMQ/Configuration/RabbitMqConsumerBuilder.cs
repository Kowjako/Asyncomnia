namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

public sealed class RabbitMqConsumerBuilder
{
    private readonly Dictionary<string, RabbitMqSubscriptionOptions> _subscriptions = new(StringComparer.Ordinal);

    internal RabbitMqConsumerBuilder(string endpointName)
    {
        EndpointName = endpointName;
    }

    public string EndpointName { get; }

    public ushort PrefetchCount { get; set; } = 1;

    public ushort DispatcherConcurrency { get; set; } = 1;

    public bool SingleActiveConsumer { get; set; }

    public bool DeadLetteringEnabled { get; private set; } = true;

    public bool BrokerRetriesEnabled { get; private set; }

    public IReadOnlyList<TimeSpan> BrokerRetryDelays { get; private set; } =
    [
        TimeSpan.FromSeconds(30),
        TimeSpan.FromMinutes(1),
        TimeSpan.FromMinutes(5)
    ];

    public void DisableDeadLettering()
    {
        DeadLetteringEnabled = false;
    }

    public void UseBrokerRetries(params TimeSpan[] delays)
    {
        ArgumentNullException.ThrowIfNull(delays);

        if (delays.Length == 0)
        {
            throw new ArgumentException("At least one broker retry delay must be configured.", nameof(delays));
        }

        if (delays.Any(delay => delay < TimeSpan.FromMilliseconds(1)))
        {
            throw new ArgumentException("Broker retry delays must be at least 1 ms.", nameof(delays));
        }

        BrokerRetriesEnabled = true;
        BrokerRetryDelays = delays.ToArray();
    }

    public void DisableBrokerRetries()
    {
        BrokerRetriesEnabled = false;
    }

    public void ConfigureSubscription(string exchangeName, Action<RabbitMqSubscriptionOptions> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(exchangeName);
        ArgumentNullException.ThrowIfNull(configure);

        if (!_subscriptions.TryGetValue(exchangeName, out var subscription))
        {
            subscription = new RabbitMqSubscriptionOptions();
            _subscriptions.Add(exchangeName, subscription);
        }

        configure(subscription);
    }

    internal void ApplyTo(RabbitMqConsumerOptions options)
    {
        options.EndpointName = EndpointName;
        options.DefaultPrefetchCount = PrefetchCount;
        options.DefaultDispatcherConcurrency = DispatcherConcurrency;
        options.DefaultSingleActiveConsumer = SingleActiveConsumer;
        options.DeadLetteringEnabled = DeadLetteringEnabled;
        options.BrokerRetriesEnabled = BrokerRetriesEnabled;
        options.BrokerRetryDelays = BrokerRetryDelays.ToArray();

        foreach (var (exchangeName, configured) in _subscriptions)
        {
            options.ConfigureSubscription(
                exchangeName,
                subscription =>
                {
                    subscription.SingleActiveConsumer = configured.SingleActiveConsumer;
                    subscription.PrefetchCount = configured.PrefetchCount;
                    subscription.DispatcherConcurrency = configured.DispatcherConcurrency;
                    subscription.QueueType = configured.QueueType;
                });
        }
    }
}
