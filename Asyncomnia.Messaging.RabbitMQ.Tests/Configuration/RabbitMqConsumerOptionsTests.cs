using Asyncomnia.Messaging.RabbitMQ.Configuration;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Configuration;

public sealed class RabbitMqConsumerOptionsTests
{
    [Fact]
    public void GetConsumerSettings_WithoutOverride_ReturnsDefaults()
    {
        var options = new RabbitMqConsumerOptions
        {
            DefaultConsumerCount = 2,
            DefaultPrefetchCount = 3,
            DefaultDispatcherConcurrency = 4
        };

        var settings = options.GetConsumerSettings("events/clients");

        Assert.Equal(2, settings.ConsumerCount);
        Assert.Equal((ushort)3, settings.PrefetchCount);
        Assert.Equal((ushort)4, settings.DispatcherConcurrency);
    }

    [Fact]
    public void GetConsumerSettings_WithPartialOverride_InheritsRemainingDefaults()
    {
        var options = new RabbitMqConsumerOptions
        {
            DefaultConsumerCount = 2,
            DefaultPrefetchCount = 3,
            DefaultDispatcherConcurrency = 4
        };
        options.ConfigureSubscription(
            "commands/long-work-job",
            subscription => subscription.ConsumerCount = 5);

        var settings = options.GetConsumerSettings("commands/long-work-job");

        Assert.Equal(5, settings.ConsumerCount);
        Assert.Equal((ushort)3, settings.PrefetchCount);
        Assert.Equal((ushort)4, settings.DispatcherConcurrency);
    }

    [Fact]
    public void ConfigureSubscription_WhenCalledTwice_MergesConfiguration()
    {
        var options = new RabbitMqConsumerOptions();
        options.ConfigureSubscription(
            "events/clients",
            subscription => subscription.ConsumerCount = 2);
        options.ConfigureSubscription(
            "events/clients",
            subscription => subscription.PrefetchCount = 7);

        var settings = options.GetConsumerSettings("events/clients");

        Assert.Equal(2, settings.ConsumerCount);
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
            subscription => subscription.ConsumerCount = 2);
        options.ConfigureSubscription(
            "events/vip",
            subscription => subscription.ConsumerCount = 3);

        var clients = options.GetConsumerSettings("events/clients");
        var vip = options.GetConsumerSettings("events/vip");

        Assert.Equal(2, clients.ConsumerCount);
        Assert.Equal(3, vip.ConsumerCount);
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
    public void GetConsumerSettings_WhenExchangeNameIsMissing_Throws(
        string? exchangeName)
    {
        var options = new RabbitMqConsumerOptions();

        Assert.ThrowsAny<ArgumentException>(
            () => options.GetConsumerSettings(exchangeName!));
    }
}
