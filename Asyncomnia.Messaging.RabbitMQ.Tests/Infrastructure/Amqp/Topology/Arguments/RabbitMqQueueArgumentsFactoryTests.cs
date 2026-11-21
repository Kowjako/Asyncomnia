using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology.Arguments;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Infrastructure.Amqp.Topology.Arguments;

public sealed class RabbitMqQueueArgumentsFactoryTests
{
    [Theory]
    [InlineData(RabbitMqQueueType.Classic, "classic")]
    [InlineData(RabbitMqQueueType.Quorum, "quorum")]
    public void Create_WithQueueType_MapsQueueType(
        RabbitMqQueueType queueType,
        string expected)
    {
        var settings = new RabbitMqQueueDeclarationSettings(queueType);

        var arguments = RabbitMqQueueArgumentsFactory.Create(settings);

        var argument = Assert.Single(arguments);
        Assert.Equal("x-queue-type", argument.Key);
        Assert.Equal(expected, argument.Value);
    }

    [Fact]
    public void Create_WithAllSettings_MapsEveryArgument()
    {
        var settings = new RabbitMqQueueDeclarationSettings(
            QueueType: RabbitMqQueueType.Quorum,
            SingleActiveConsumer: true,
            MessageTtl: TimeSpan.FromMilliseconds(1_500),
            DeadLetterExchange: "asyncomnia/retry-return",
            MaxLength: 25,
            MaxLengthBytes: 4_096,
            Overflow: RabbitMqQueueOverflowStrategy.DropHead);

        var arguments = RabbitMqQueueArgumentsFactory.Create(settings);

        Assert.Equal(7, arguments.Count);
        Assert.Equal("quorum", arguments["x-queue-type"]);
        Assert.Equal(true, arguments["x-single-active-consumer"]);
        Assert.Equal(1_500L, arguments["x-message-ttl"]);
        Assert.Equal(
            "asyncomnia/retry-return",
            arguments["x-dead-letter-exchange"]);
        Assert.Equal(25, arguments["x-max-length"]);
        Assert.Equal(4_096L, arguments["x-max-length-bytes"]);
        Assert.Equal("drop-head", arguments["x-overflow"]);
    }

    [Theory]
    [InlineData((int)RabbitMqQueueOverflowStrategy.DropHead, "drop-head")]
    [InlineData((int)RabbitMqQueueOverflowStrategy.RejectPublish, "reject-publish")]
    public void Create_WithOverflowStrategy_MapsOverflow(
        int overflow,
        string expected)
    {
        var settings = new RabbitMqQueueDeclarationSettings(
            RabbitMqQueueType.Quorum,
            Overflow: (RabbitMqQueueOverflowStrategy)overflow);

        var arguments = RabbitMqQueueArgumentsFactory.Create(settings);

        Assert.Equal(expected, arguments["x-overflow"]);
    }

    [Fact]
    public void Create_ReturnsIndependentDictionary()
    {
        var settings = new RabbitMqQueueDeclarationSettings(
            RabbitMqQueueType.Quorum);

        var first = RabbitMqQueueArgumentsFactory.Create(settings);
        var second = RabbitMqQueueArgumentsFactory.Create(settings);

        Assert.NotSame(first, second);
    }

    [Fact]
    public void Create_WhenSettingsAreNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            RabbitMqQueueArgumentsFactory.Create(null!));

        Assert.Equal("settings", exception.ParamName);
    }

    [Fact]
    public void Create_WhenQueueTypeIsInvalid_ThrowsArgumentOutOfRangeException()
    {
        var settings = new RabbitMqQueueDeclarationSettings(
            (RabbitMqQueueType)int.MaxValue);

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            RabbitMqQueueArgumentsFactory.Create(settings));

        Assert.Equal("QueueType", exception.ParamName);
    }

    [Fact]
    public void Create_WhenOverflowStrategyIsInvalid_ThrowsArgumentOutOfRangeException()
    {
        var settings = new RabbitMqQueueDeclarationSettings(
            RabbitMqQueueType.Quorum,
            Overflow: (RabbitMqQueueOverflowStrategy)int.MaxValue);

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            RabbitMqQueueArgumentsFactory.Create(settings));

        Assert.Equal("Overflow", exception.ParamName);
    }
}
