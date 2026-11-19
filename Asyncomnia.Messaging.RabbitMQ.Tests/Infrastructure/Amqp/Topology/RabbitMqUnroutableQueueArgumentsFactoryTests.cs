using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Infrastructure.Amqp.Topology;

public sealed class RabbitMqUnroutableQueueArgumentsFactoryTests
{
    [Fact]
    public void Create_ReturnsConfiguredLimitsAndDropHeadOverflow()
    {
        var options = new RabbitMqOptions
        {
            UnroutableQueueMaxLength = 25,
            UnroutableQueueMaxLengthBytes = 4096
        };

        var arguments =
            RabbitMqUnroutableQueueArgumentsFactory.Create(options);

        Assert.Equal(3, arguments.Count);
        Assert.Equal(25, arguments["x-max-length"]);
        Assert.Equal(4096L, arguments["x-max-length-bytes"]);
        Assert.Equal("drop-head", arguments["x-overflow"]);
    }

    [Fact]
    public void Create_ReturnsIndependentDictionary()
    {
        var options = new RabbitMqOptions();

        var first = RabbitMqUnroutableQueueArgumentsFactory.Create(options);
        var second = RabbitMqUnroutableQueueArgumentsFactory.Create(options);

        Assert.NotSame(first, second);
    }

    [Fact]
    public void Create_WhenOptionsAreNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            RabbitMqUnroutableQueueArgumentsFactory.Create(null!));

        Assert.Equal("options", exception.ParamName);
    }
}
