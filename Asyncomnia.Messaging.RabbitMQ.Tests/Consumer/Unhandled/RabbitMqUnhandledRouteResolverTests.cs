using Asyncomnia.Messaging.RabbitMQ.Consumer.Unhandled;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.Unhandled;

public sealed class RabbitMqUnhandledRouteResolverTests
{
    [Fact]
    public void Resolve_WithSourceQueueName_ReturnsUnhandledRoute()
    {
        var resolver = new RabbitMqUnhandledRouteResolver();

        var route = resolver.Resolve(
            "events/clients/subscription/billing-service");

        Assert.Equal("asyncomnia/unhandled", route.ExchangeName);
        Assert.Equal(
            "unhandled/events/clients/subscription/billing-service",
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
        var resolver = new RabbitMqUnhandledRouteResolver();

        Assert.ThrowsAny<ArgumentException>(
            () => resolver.Resolve(sourceQueueName!));
    }
}
