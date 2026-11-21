using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology.Arguments;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Infrastructure.Amqp.Topology.Arguments;

public sealed class RabbitMqExchangeArgumentsFactoryTests
{
    [Fact]
    public void Create_WithoutUnroutableFallback_ReturnsNull()
    {
        var arguments = RabbitMqExchangeArgumentsFactory.Create(false);

        Assert.Null(arguments);
    }

    [Fact]
    public void Create_WithUnroutableFallback_ReturnsAlternateExchangeArgument()
    {
        var arguments = RabbitMqExchangeArgumentsFactory.Create(true);

        Assert.NotNull(arguments);
        var argument = Assert.Single(arguments);
        Assert.Equal("alternate-exchange", argument.Key);
        Assert.Equal(RabbitMqUnroutableDefaults.ExchangeName, argument.Value);
    }

    [Fact]
    public void Create_WithUnroutableFallback_ReturnsIndependentDictionary()
    {
        var first = RabbitMqExchangeArgumentsFactory.Create(true);
        var second = RabbitMqExchangeArgumentsFactory.Create(true);

        Assert.NotSame(first, second);
    }
}
