using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;
using Asyncomnia.Messaging.RabbitMQ.Routing;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using EasyNetQ;
using EasyNetQ.Topology;
using Microsoft.Extensions.Options;
using Moq;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.Topology;

public sealed class RabbitMqTopologyCreatorTests
{
    [Fact]
    public async Task DeclareAsync_WithoutSubscriptions_ReturnsEmptyCollectionWithoutCallingEasyNetQ()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy = new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var creator = CreateCreator(advancedBus, exchangeDeclareStrategy);

        var declaredSubscriptions = await creator.DeclareAsync();

        Assert.Empty(declaredSubscriptions);
        advancedBus.VerifyNoOtherCalls();
        exchangeDeclareStrategy.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task DeclareAsync_WithMultipleBindings_DeclaresDurableTopologyAndReturnsSubscription()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy = new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var creator = CreateCreator(
            advancedBus,
            exchangeDeclareStrategy,
            eventTypes:
            [
                typeof(ClientCreatedEvent),
                typeof(ClientContractSignedEvent)
            ]);
        var exchange = CreateExchange("events/clients");
        var queue = CreateQueue("events/clients/subscription/test-service");
        Action<IQueueDeclareConfiguration>? configureQueue = null;
        exchangeDeclareStrategy
            .Setup(x => x.DeclareExchangeAsync(
                exchange.Name,
                ExchangeType.Topic,
                CancellationToken.None))
            .ReturnsAsync(exchange);
        advancedBus
            .Setup(x => x.QueueDeclareAsync(
                queue.Name,
                It.IsAny<Action<IQueueDeclareConfiguration>>(),
                CancellationToken.None))
            .Callback<string, Action<IQueueDeclareConfiguration>, CancellationToken>(
                (_, configure, _) => configureQueue = configure)
            .ReturnsAsync(queue);
        advancedBus
            .Setup(x => x.QueueBindAsync(
                queue.Name,
                exchange.Name,
                "client-contract-signed",
                null,
                CancellationToken.None))
            .Returns(Task.CompletedTask);
        advancedBus
            .Setup(x => x.QueueBindAsync(
                queue.Name,
                exchange.Name,
                "client-created-event",
                null,
                CancellationToken.None))
            .Returns(Task.CompletedTask);

        var declaredSubscriptions = await creator.DeclareAsync();

        Assert.Single(declaredSubscriptions);
        Assert.NotNull(configureQueue);
        var queueConfiguration = new Mock<IQueueDeclareConfiguration>(MockBehavior.Strict);
        queueConfiguration
            .Setup(x => x.AsDurable(true))
            .Returns(queueConfiguration.Object);
        queueConfiguration
            .Setup(x => x.AsAutoDelete(false))
            .Returns(queueConfiguration.Object);
        queueConfiguration
            .Setup(x => x.AsExclusive(false))
            .Returns(queueConfiguration.Object);

        configureQueue(queueConfiguration.Object);

        queueConfiguration.VerifyAll();
        exchangeDeclareStrategy.VerifyAll();
        advancedBus.VerifyAll();
    }

    [Fact]
    public async Task DeclareAsync_WithEventAndCommand_DeclaresSeparateTopologies()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy = new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var creator = CreateCreator(
            advancedBus,
            exchangeDeclareStrategy,
            eventTypes: [typeof(ClientCreatedEvent)],
            commandTypes: [typeof(StartLongWorkJobCommand)]);
        var commandExchange = CreateExchange("commands/long-work-job");
        var commandQueue = CreateQueue("commands/long-work-job/subscription/test-service");
        var eventExchange = CreateExchange("events/clients");
        var eventQueue = CreateQueue("events/clients/subscription/test-service");
        SetupTopology(
            advancedBus,
            exchangeDeclareStrategy,
            commandExchange,
            commandQueue,
            "start-long-work-job-command");
        SetupTopology(
            advancedBus,
            exchangeDeclareStrategy,
            eventExchange,
            eventQueue,
            "client-created-event");

        var declaredSubscriptions = await creator.DeclareAsync();

        Assert.Equal(2, declaredSubscriptions.Count);
        exchangeDeclareStrategy.VerifyAll();
        advancedBus.VerifyAll();
    }

    [Fact]
    public async Task DeclareAsync_WhenCancellationIsAlreadyRequested_DoesNotCallEasyNetQ()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy = new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var creator = CreateCreator(
            advancedBus,
            exchangeDeclareStrategy,
            eventTypes: [typeof(ClientCreatedEvent)]);
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => creator.DeclareAsync(cancellationTokenSource.Token));

        advancedBus.VerifyNoOtherCalls();
        exchangeDeclareStrategy.VerifyNoOtherCalls();
    }

    private static RabbitMqTopologyCreator CreateCreator(
        Mock<IAdvancedBus> advancedBus,
        Mock<IExchangeDeclareStrategy> exchangeDeclareStrategy,
        IReadOnlyCollection<Type>? eventTypes = null,
        IReadOnlyCollection<Type>? commandTypes = null)
    {
        var catalog = new Mock<IMessageHandlerCatalog>(MockBehavior.Strict);
        catalog
            .SetupGet(x => x.EventTypes)
            .Returns(eventTypes ?? Array.Empty<Type>());
        catalog
            .SetupGet(x => x.CommandTypes)
            .Returns(commandTypes ?? Array.Empty<Type>());
        var subscriptionResolver = new RabbitMqSubscriptionResolver(
            catalog.Object,
            new MessageRouteResolver(),
            Options.Create(new RabbitMqConsumerOptions
            {
                EndpointName = "test-service"
            }));

        return new RabbitMqTopologyCreator(
            advancedBus.Object,
            exchangeDeclareStrategy.Object,
            subscriptionResolver);
    }

    private static void SetupTopology(
        Mock<IAdvancedBus> advancedBus,
        Mock<IExchangeDeclareStrategy> exchangeDeclareStrategy,
        Exchange exchange,
        Queue queue,
        string routingKey)
    {
        exchangeDeclareStrategy
            .Setup(x => x.DeclareExchangeAsync(
                exchange.Name,
                ExchangeType.Topic,
                CancellationToken.None))
            .ReturnsAsync(exchange);
        advancedBus
            .Setup(x => x.QueueDeclareAsync(
                queue.Name,
                It.IsAny<Action<IQueueDeclareConfiguration>>(),
                CancellationToken.None))
            .ReturnsAsync(queue);
        advancedBus
            .Setup(x => x.QueueBindAsync(
                queue.Name,
                exchange.Name,
                routingKey,
                null,
                CancellationToken.None))
            .Returns(Task.CompletedTask);
    }

    private static Exchange CreateExchange(string name)
    {
        return new Exchange(
            name,
            ExchangeType.Topic,
            true,
            false,
            new Dictionary<string, object>());
    }

    private static Queue CreateQueue(string name)
    {
        return new Queue(
            name,
            true,
            false,
            false,
            new Dictionary<string, object>());
    }
}
