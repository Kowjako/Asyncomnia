using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.DeadLettering;

public sealed class RabbitMqDeadLetterRouteResolverTests
{
    [Fact]
    public void Resolve_WithSourceQueueName_ReturnsDeadLetterRoute()
    {
        var resolver = new RabbitMqDeadLetterRouteResolver();

        var route = resolver.Resolve(
            "events/clients/subscription/billing-service");

        Assert.Equal("asyncomnia/deadletter", route.ExchangeName);
        Assert.Equal(
            "deadletter/events/clients/subscription/billing-service",
            route.QueueName);
        Assert.Equal(route.QueueName, route.RoutingKey);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Resolve_WhenSourceQueueNameIsMissing_Throws(
        string? sourceQueueName)
    {
        var resolver = new RabbitMqDeadLetterRouteResolver();

        Assert.ThrowsAny<ArgumentException>(
            () => resolver.Resolve(sourceQueueName!));
    }
}
