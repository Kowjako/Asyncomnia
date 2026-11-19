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
            builder.ConfigureConsumer(endpointName!, _ => { }));
    }

    [Fact]
    public void ConfigureConsumer_WithNullConfiguration_Throws()
    {
        var builder = new RabbitMqBuilder();

        Assert.Throws<ArgumentNullException>(() =>
            builder.ConfigureConsumer("billing-service", null!));
    }

    [Fact]
    public void ConfigureConsumer_WhenAlreadyConfigured_Throws()
    {
        var builder = new RabbitMqBuilder();
        builder.ConfigureConsumer("billing-service", _ => { });

        var exception = Assert.Throws<InvalidOperationException>(() =>
            builder.ConfigureConsumer("other-service", _ => { }));

        Assert.Contains("already been configured", exception.Message);
    }
}
