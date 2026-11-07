using Asyncomnia.Messaging.RabbitMQ.Routing;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using Asyncomnia.Messaging.Routing;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Routing;

public sealed class MessageRouteResolverTests
{
    private readonly MessageRouteResolver _resolver = new();

    [Fact]
    public void Resolve_ForEvent_ReturnsEventsExchangeAndKebabCaseRoutingKey()
    {
        var route = _resolver.Resolve(typeof(ClientCreatedEvent));

        Assert.Equal("events/clients", route.ExchangeName);
        Assert.Equal("client-created-event", route.RoutingKey);
    }

    [Fact]
    public void Resolve_ForCommand_ReturnsCommandsExchangeAndKebabCaseRoutingKey()
    {
        var route = _resolver.Resolve(typeof(StartLongWorkJobCommand));

        Assert.Equal("commands/long-work-job", route.ExchangeName);
        Assert.Equal("start-long-work-job-command", route.RoutingKey);
    }

    [Fact]
    public void Resolve_WithExplicitRoutingKey_UsesConfiguredRoutingKey()
    {
        var route = _resolver.Resolve(typeof(ClientContractSignedEvent));

        Assert.Equal("events/clients", route.ExchangeName);
        Assert.Equal("client-contract-signed", route.RoutingKey);
    }

    [Fact]
    public void Resolve_WithAcronymInTypeName_ProducesReadableKebabCaseRoutingKey()
    {
        var route = _resolver.Resolve(typeof(HTTPResponseReceivedEvent));

        Assert.Equal("http-response-received-event", route.RoutingKey);
    }

    [Fact]
    public void Resolve_CalledTwiceForSameType_ReturnsCachedRoute()
    {
        var firstRoute = _resolver.Resolve(typeof(ClientCreatedEvent));

        var secondRoute = _resolver.Resolve(typeof(ClientCreatedEvent));

        Assert.Same(firstRoute, secondRoute);
    }

    [Fact]
    public void Resolve_WhenMessageDoesNotDefineRoute_ThrowsInvalidOperationException()
    {
        var exception = Assert.Throws<InvalidOperationException>(
            () => _resolver.Resolve(typeof(MessageWithoutRoute)));

        Assert.Contains(nameof(MessageWithoutRoute), exception.Message);
        Assert.Contains(nameof(MessageRouteAttribute), exception.Message);
    }

    [Theory]
    [InlineData(typeof(AmbiguousMessage))]
    [InlineData(typeof(MessageWithoutKind))]
    public void Resolve_WhenMessageDoesNotImplementExactlyOneMessageKind_ThrowsInvalidOperationException(Type messageType)
    {
        var exception = Assert.Throws<InvalidOperationException>(
            () => _resolver.Resolve(messageType));

        Assert.Contains("exactly one", exception.Message);
    }

    [Fact]
    public void Resolve_WithNullMessageType_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => _resolver.Resolve(null!));
    }
}
