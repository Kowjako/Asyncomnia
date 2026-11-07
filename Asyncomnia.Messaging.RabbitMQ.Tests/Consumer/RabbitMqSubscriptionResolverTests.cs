using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Routing;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using Microsoft.Extensions.Options;
using Moq;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer;

public sealed class RabbitMqSubscriptionResolverTests
{
    [Fact]
    public void Resolve_WithoutHandledMessageTypes_ReturnsEmptyCollection()
    {
        var resolver = CreateResolver();

        var subscriptions = resolver.Resolve();

        Assert.Empty(subscriptions);
    }

    [Fact]
    public void Resolve_WithEventsOnSameExchange_CreatesOneSubscriptionWithMultipleBindings()
    {
        var resolver = CreateResolver(
            eventTypes:
            [
                typeof(ClientCreatedEvent),
                typeof(ClientContractSignedEvent)
            ],
            endpointName: "auditing-service");

        var subscriptions = resolver.Resolve();

        var subscription = Assert.Single(subscriptions);
        Assert.Equal("events/clients", subscription.ExchangeName);
        Assert.Equal(
            "events/clients/subscription/auditing-service",
            subscription.QueueName);
        Assert.Collection(
            subscription.Bindings,
            binding =>
            {
                Assert.Equal("client-contract-signed", binding.RoutingKey);
                Assert.Equal(typeof(ClientContractSignedEvent), binding.MessageType);
            },
            binding =>
            {
                Assert.Equal("client-created-event", binding.RoutingKey);
                Assert.Equal(typeof(ClientCreatedEvent), binding.MessageType);
            });
    }

    [Fact]
    public void Resolve_WithEventAndCommand_CreatesSeparateOrderedSubscriptions()
    {
        var resolver = CreateResolver(
            eventTypes: [typeof(ClientCreatedEvent)],
            commandTypes: [typeof(StartLongWorkJobCommand)],
            endpointName: "worker-service");

        var subscriptions = resolver.Resolve();

        Assert.Collection(
            subscriptions,
            subscription =>
            {
                Assert.Equal("commands/long-work-job", subscription.ExchangeName);
                Assert.Equal(
                    "commands/long-work-job/subscription/worker-service",
                    subscription.QueueName);
                var binding = Assert.Single(subscription.Bindings);
                Assert.Equal("start-long-work-job-command", binding.RoutingKey);
                Assert.Equal(typeof(StartLongWorkJobCommand), binding.MessageType);
            },
            subscription =>
            {
                Assert.Equal("events/clients", subscription.ExchangeName);
                Assert.Equal(
                    "events/clients/subscription/worker-service",
                    subscription.QueueName);
                var binding = Assert.Single(subscription.Bindings);
                Assert.Equal("client-created-event", binding.RoutingKey);
                Assert.Equal(typeof(ClientCreatedEvent), binding.MessageType);
            });
    }

    [Fact]
    public void Resolve_WithEventsOnDifferentExchanges_CreatesSeparateSubscriptions()
    {
        var resolver = CreateResolver(
            eventTypes:
            [
                typeof(HTTPResponseReceivedEvent),
                typeof(ClientCreatedEvent)
            ],
            endpointName: "integration-service");

        var subscriptions = resolver.Resolve();

        Assert.Collection(
            subscriptions,
            subscription =>
            {
                Assert.Equal("events/clients", subscription.ExchangeName);
                Assert.Equal(
                    "events/clients/subscription/integration-service",
                    subscription.QueueName);
            },
            subscription =>
            {
                Assert.Equal("events/http", subscription.ExchangeName);
                Assert.Equal(
                    "events/http/subscription/integration-service",
                    subscription.QueueName);
            });
    }

    private static RabbitMqSubscriptionResolver CreateResolver(IReadOnlyCollection<Type>? eventTypes = null, IReadOnlyCollection<Type>? commandTypes = null, string endpointName = "test-service")
    {
        var catalog = new Mock<IMessageHandlerCatalog>(MockBehavior.Strict);
        catalog
            .SetupGet(x => x.EventTypes)
            .Returns(eventTypes ?? Array.Empty<Type>());
        catalog
            .SetupGet(x => x.CommandTypes)
            .Returns(commandTypes ?? Array.Empty<Type>());

        return new RabbitMqSubscriptionResolver(
            catalog.Object,
            new MessageRouteResolver(),
            Options.Create(new RabbitMqConsumerOptions
            {
                EndpointName = endpointName
            }));
    }
}
