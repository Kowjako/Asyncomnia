using Asyncomnia.Messaging.RabbitMQ.Configuration;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Configuration;

public sealed class RabbitMqOptionsTests
{
    [Fact]
    public void CaptureUnroutableEvents_DefaultsToEnabled()
    {
        var options = new RabbitMqOptions();

        Assert.True(options.CaptureUnroutableEvents);
    }

    [Fact]
    public void UnroutableQueueLimits_UseBoundedDefaults()
    {
        var options = new RabbitMqOptions();

        Assert.Equal(10_000, options.UnroutableQueueMaxLength);
        Assert.Equal(
            256L * 1024 * 1024,
            options.UnroutableQueueMaxLengthBytes);
    }
}
