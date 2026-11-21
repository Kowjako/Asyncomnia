using Asyncomnia.Messaging.RabbitMQ.Configuration;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Configuration;

public sealed class RabbitMqConsumerBuilderTests
{
    [Fact]
    public void Constructor_UsesExpectedDefaults()
    {
        var builder = new RabbitMqConsumerBuilder("billing-service");

        Assert.Equal("billing-service", builder.EndpointName);
        Assert.Equal((ushort)1, builder.PrefetchCount);
        Assert.Equal((ushort)1, builder.DispatcherConcurrency);
        Assert.False(builder.SingleActiveConsumer);
        Assert.True(builder.DeadLetteringEnabled);
        Assert.False(builder.BrokerRetriesEnabled);
        Assert.Equal(
            [
                TimeSpan.FromSeconds(30),
                TimeSpan.FromMinutes(1),
                TimeSpan.FromMinutes(5)
            ],
            builder.BrokerRetryDelays);
    }

    [Fact]
    public void DisableDeadLettering_DisablesDeadLettering()
    {
        var builder = new RabbitMqConsumerBuilder("billing-service");

        builder.DisableDeadLettering();

        Assert.False(builder.DeadLetteringEnabled);
    }

    [Fact]
    public void UseBrokerRetries_EnablesRetriesAndCopiesDelays()
    {
        var delays = new[]
        {
            TimeSpan.FromSeconds(2),
            TimeSpan.FromSeconds(5)
        };
        var builder = new RabbitMqConsumerBuilder("billing-service");

        builder.UseBrokerRetries(delays);
        delays[0] = TimeSpan.FromDays(1);

        Assert.True(builder.BrokerRetriesEnabled);
        Assert.Equal(
            [TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5)],
            builder.BrokerRetryDelays);
    }

    [Fact]
    public void DisableBrokerRetries_DisablesRetriesAndPreservesSchedule()
    {
        var builder = new RabbitMqConsumerBuilder("billing-service");
        builder.UseBrokerRetries(TimeSpan.FromSeconds(2));

        builder.DisableBrokerRetries();

        Assert.False(builder.BrokerRetriesEnabled);
        Assert.Equal([TimeSpan.FromSeconds(2)], builder.BrokerRetryDelays);
    }

    [Fact]
    public void UseBrokerRetries_WithoutDelays_Throws()
    {
        var builder = new RabbitMqConsumerBuilder("billing-service");

        Assert.Throws<ArgumentException>(() =>
            builder.UseBrokerRetries());
    }

    [Fact]
    public void UseBrokerRetries_WithNullDelays_Throws()
    {
        var builder = new RabbitMqConsumerBuilder("billing-service");

        Assert.Throws<ArgumentNullException>(() =>
            builder.UseBrokerRetries(null!));
    }

    [Fact]
    public void UseBrokerRetries_WithDelayShorterThanOneMillisecond_Throws()
    {
        var builder = new RabbitMqConsumerBuilder("billing-service");

        Assert.Throws<ArgumentException>(() =>
            builder.UseBrokerRetries(TimeSpan.FromTicks(1)));
    }

    [Fact]
    public void ConfigureSubscription_WhenCalledTwice_MergesConfiguration()
    {
        var builder = new RabbitMqConsumerBuilder("billing-service");
        builder.ConfigureSubscription(
            "events/orders",
            options => options.SingleActiveConsumer = true);
        builder.ConfigureSubscription(
            "events/orders",
            options => options.PrefetchCount = 17);
        var runtimeOptions = new RabbitMqConsumerOptions();

        builder.ApplyTo(runtimeOptions);

        var subscription = runtimeOptions.GetSubscriptionSettings("events/orders");
        Assert.True(subscription.SingleActiveConsumer);
        Assert.Equal((ushort)17, subscription.PrefetchCount);
        Assert.Single(runtimeOptions.Subscriptions);
    }

    [Fact]
    public void ApplyTo_WithSubscriptionQueueType_CopiesQueueTypeOverride()
    {
        var builder = new RabbitMqConsumerBuilder("billing-service");
        builder.ConfigureSubscription(
            "events/orders",
            options => options.QueueType = RabbitMqQueueType.Classic);
        var runtimeOptions = new RabbitMqConsumerOptions();

        builder.ApplyTo(runtimeOptions);

        var subscription = runtimeOptions.GetSubscriptionSettings("events/orders");
        Assert.Equal(RabbitMqQueueType.Classic, subscription.QueueType);
    }
}
