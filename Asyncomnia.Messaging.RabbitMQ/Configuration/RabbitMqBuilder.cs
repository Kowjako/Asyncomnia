namespace Asyncomnia.Messaging.RabbitMQ.Configuration;

public sealed class RabbitMqBuilder
{
    private RabbitMqConsumerBuilder? _consumer;

    internal RabbitMqBuilder() { }

    public string ConnectionString { get; set; } = string.Empty;

    public int PublisherChannelCount { get; set; } = 4;

    public bool CaptureUnroutableEventsEnabled { get; private set; } = true;

    public int UnroutableQueueMaxLength { get; private set; } = 10_000;

    public long UnroutableQueueMaxLengthBytes { get; private set; } = 256L * 1024 * 1024;

    internal bool ConsumerConfigured => _consumer is not null;

    public void CaptureUnroutableEvents(int maxLength, long maxBytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxLength, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxBytes, 1);

        CaptureUnroutableEventsEnabled = true;
        UnroutableQueueMaxLength = maxLength;
        UnroutableQueueMaxLengthBytes = maxBytes;
    }

    public void DisableUnroutableEventCapture()
    {
        CaptureUnroutableEventsEnabled = false;
    }

    public void ConfigureConsumer(string endpointName, Action<RabbitMqConsumerBuilder> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpointName);
        ArgumentNullException.ThrowIfNull(configure);

        if (_consumer is not null)
        {
            throw new InvalidOperationException("RabbitMQ consumer has already been configured.");
        }

        var consumer = new RabbitMqConsumerBuilder(endpointName);

        configure(consumer);

        _consumer = consumer;
    }

    internal void ApplyTo(RabbitMqOptions options)
    {
        options.ConnectionString = ConnectionString;
        options.PublisherChannelCount = PublisherChannelCount;
        options.CaptureUnroutableEvents = CaptureUnroutableEventsEnabled;
        options.UnroutableQueueMaxLength = UnroutableQueueMaxLength;
        options.UnroutableQueueMaxLengthBytes = UnroutableQueueMaxLengthBytes;
    }

    internal void ApplyConsumerTo(RabbitMqConsumerOptions options)
    {
        if (_consumer is null)
        {
            throw new InvalidOperationException("RabbitMQ consumer is not configured.");
        }

        _consumer.ApplyTo(options);
    }
}
