using Asyncomnia.Messaging.RabbitMQ.Configuration;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Configuration;

public sealed class RabbitMqConsumerOptionsTests
{
    [Fact]
    public void DeadLetteringEnabled_ByDefault_IsTrue()
    {
        var options = new RabbitMqConsumerOptions();

        Assert.True(options.DeadLetteringEnabled);
    }

    [Fact]
    public void SingleActiveConsumer_ByDefault_IsDisabled()
    {
        var options = new RabbitMqConsumerOptions();

        Assert.False(options.DefaultSingleActiveConsumer);
    }

    [Fact]
    public void BrokerRetries_ByDefault_AreDisabledWithDefaultDelaySchedule()
    {
        var options = new RabbitMqConsumerOptions();

        Assert.False(options.BrokerRetriesEnabled);
        Assert.Equal(
            [
                TimeSpan.FromSeconds(30),
                TimeSpan.FromMinutes(1),
                TimeSpan.FromMinutes(5)
            ],
            options.BrokerRetryDelays);
    }

    [Fact]
    public void GetSubscriptionSettings_WithoutOverride_ReturnsDefaults()
    {
        var options = new RabbitMqConsumerOptions
        {
            DefaultSingleActiveConsumer = true,
            DefaultPrefetchCount = 3,
            DefaultDispatcherConcurrency = 4
        };

        var settings = options.GetSubscriptionSettings("events/clients");

        Assert.True(settings.SingleActiveConsumer);
        Assert.Equal((ushort)3, settings.PrefetchCount);
        Assert.Equal((ushort)4, settings.DispatcherConcurrency);
    }

    [Fact]
    public void GetSubscriptionSettings_WithPartialOverride_InheritsRemainingDefaults()
    {
        var options = new RabbitMqConsumerOptions
        {
            DefaultSingleActiveConsumer = true,
            DefaultPrefetchCount = 3,
            DefaultDispatcherConcurrency = 4
        };
        options.ConfigureSubscription(
            "commands/long-work-job",
            subscription => subscription.SingleActiveConsumer = false);

        var settings = options.GetSubscriptionSettings("commands/long-work-job");

        Assert.False(settings.SingleActiveConsumer);
        Assert.Equal((ushort)3, settings.PrefetchCount);
        Assert.Equal((ushort)4, settings.DispatcherConcurrency);
    }

    [Fact]
    public void ConfigureSubscription_WhenCalledTwice_MergesConfiguration()
    {
        var options = new RabbitMqConsumerOptions();
        options.ConfigureSubscription(
            "events/clients",
            subscription => subscription.SingleActiveConsumer = true);
        options.ConfigureSubscription(
            "events/clients",
            subscription => subscription.PrefetchCount = 7);

        var settings = options.GetSubscriptionSettings("events/clients");

        Assert.True(settings.SingleActiveConsumer);
        Assert.Equal((ushort)7, settings.PrefetchCount);
        Assert.Equal(options.DefaultDispatcherConcurrency, settings.DispatcherConcurrency);
        Assert.Single(options.Subscriptions);
    }

    [Fact]
    public void ConfigureSubscription_ForDifferentExchanges_KeepsIndependentSettings()
    {
        var options = new RabbitMqConsumerOptions();
        options.ConfigureSubscription(
            "events/clients",
            subscription => subscription.SingleActiveConsumer = true);
        options.ConfigureSubscription(
            "events/vip",
            subscription => subscription.PrefetchCount = 3);

        var clients = options.GetSubscriptionSettings("events/clients");
        var vip = options.GetSubscriptionSettings("events/vip");

        Assert.True(clients.SingleActiveConsumer);
        Assert.Equal(options.DefaultPrefetchCount, clients.PrefetchCount);
        Assert.False(vip.SingleActiveConsumer);
        Assert.Equal((ushort)3, vip.PrefetchCount);
        Assert.Equal(2, options.Subscriptions.Count);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ConfigureSubscription_WhenExchangeNameIsMissing_Throws(string? exchangeName)
    {
        var options = new RabbitMqConsumerOptions();

        Assert.ThrowsAny<ArgumentException>(
            () => options.ConfigureSubscription(exchangeName!, _ => { }));
    }

    [Fact]
    public void ConfigureSubscription_WhenConfigureIsNull_Throws()
    {
        var options = new RabbitMqConsumerOptions();

        Assert.Throws<ArgumentNullException>(
            () => options.ConfigureSubscription("events/clients", null!));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void GetSubscriptionSettings_WhenExchangeNameIsMissing_Throws(
        string? exchangeName)
    {
        var options = new RabbitMqConsumerOptions();

        Assert.ThrowsAny<ArgumentException>(
            () => options.GetSubscriptionSettings(exchangeName!));
    }
}
