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
}
