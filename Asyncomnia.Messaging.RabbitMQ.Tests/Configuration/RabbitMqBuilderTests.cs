using Asyncomnia.Messaging.RabbitMQ.Configuration;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Configuration;

public sealed class RabbitMqBuilderTests
{
    [Fact]
    public void CaptureUnroutableEvents_ConfiguresAndEnablesCapture()
    {
        var builder = new RabbitMqBuilder();
        builder.DisableUnroutableEventCapture();

        builder.CaptureUnroutableEvents(321, 654);

        Assert.True(builder.CaptureUnroutableEventsEnabled);
        Assert.Equal(321, builder.UnroutableQueueMaxLength);
        Assert.Equal(654, builder.UnroutableQueueMaxLengthBytes);
    }

    [Fact]
    public void DisableUnroutableEventCapture_DisablesCapture()
    {
        var builder = new RabbitMqBuilder();

        builder.DisableUnroutableEventCapture();

        Assert.False(builder.CaptureUnroutableEventsEnabled);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(-1, 1)]
    [InlineData(1, 0)]
    [InlineData(1, -1)]
    public void CaptureUnroutableEvents_WithInvalidLimit_Throws(
        int maxLength,
        long maxBytes)
    {
        var builder = new RabbitMqBuilder();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            builder.CaptureUnroutableEvents(maxLength, maxBytes));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ConfigureConsumer_WithMissingEndpointName_Throws(
        string? endpointName)
    {
        var builder = new RabbitMqBuilder();

        Assert.ThrowsAny<ArgumentException>(() =>
            builder.ConfigureConsumer(endpointName!));
    }

    [Fact]
    public void ConfigureConsumer_WithoutConfiguration_UsesDefaults()
    {
        var builder = new RabbitMqBuilder();

        builder.ConfigureConsumer("billing-service");

        var options = new RabbitMqConsumerOptions();
        builder.ApplyConsumerTo(options);

        Assert.Equal("billing-service", options.EndpointName);
        Assert.Equal((ushort)1, options.DefaultPrefetchCount);
        Assert.Equal((ushort)1, options.DefaultDispatcherConcurrency);
        Assert.False(options.DefaultSingleActiveConsumer);
        Assert.True(options.DeadLetteringEnabled);
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
    public void ConfigureConsumer_WhenAlreadyConfigured_Throws()
    {
        var builder = new RabbitMqBuilder();
        builder.ConfigureConsumer("billing-service");

        var exception = Assert.Throws<InvalidOperationException>(() =>
            builder.ConfigureConsumer("other-service", _ => { }));

        Assert.Contains("already been configured", exception.Message);
    }
}
