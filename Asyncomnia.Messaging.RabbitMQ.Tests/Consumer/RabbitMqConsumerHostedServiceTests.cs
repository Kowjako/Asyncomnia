using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;
using Asyncomnia.Messaging.RabbitMQ.Routing;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using EasyNetQ;
using EasyNetQ.Topology;
using Microsoft.Extensions.Options;
using Moq;
using RabbitQueue = EasyNetQ.Topology.Queue;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer;

public sealed class RabbitMqConsumerHostedServiceTests
{
    [Fact]
    public async Task StartAsync_StartsConfiguredNumberOfConsumersForEachSubscription()
    {
        var options = new RabbitMqConsumerOptions
        {
            EndpointName = "worker-service"
        };
        options.ConfigureSubscription(
            "commands/long-work-job",
            subscription => subscription.ConsumerCount = 2);
        var consumerFactory = new Mock<IRabbitMqQueueConsumerFactory>(MockBehavior.Strict);
        var disposeOrder = new List<string>();
        var consumers = new System.Collections.Generic.Queue<IAsyncDisposable>(
        [
            new TrackingConsumer("command-1", disposeOrder),
            new TrackingConsumer("command-2", disposeOrder),
            new TrackingConsumer("event-1", disposeOrder)
        ]);
        var startedExchanges = new List<string>();
        consumerFactory
            .Setup(x => x.StartAsync(
                It.IsAny<RabbitMqDeclaredSubscription>(),
                CancellationToken.None))
            .Callback<RabbitMqDeclaredSubscription, CancellationToken>(
                (subscription, _) =>
                    startedExchanges.Add(subscription.Subscription.ExchangeName))
            .Returns(() => Task.FromResult(consumers.Dequeue()));
        var fixture = CreateFixture(
            consumerFactory,
            options,
            eventTypes: [typeof(ClientCreatedEvent)],
            commandTypes: [typeof(StartLongWorkJobCommand)]);

        await fixture.Service.StartAsync(CancellationToken.None);

        Assert.Equal(
            [
                "commands/long-work-job",
                "commands/long-work-job",
                "events/clients"
            ],
            startedExchanges);
        consumerFactory.Verify(
            x => x.StartAsync(
                It.IsAny<RabbitMqDeclaredSubscription>(),
                CancellationToken.None),
            Times.Exactly(3));

        await fixture.Service.StopAsync(CancellationToken.None);

        Assert.Equal(["event-1", "command-2", "command-1"], disposeOrder);
        fixture.VerifyTopology();
        consumerFactory.VerifyAll();
    }

    [Fact]
    public async Task StartAsync_WithoutSubscriptions_DoesNotStartConsumers()
    {
        var consumerFactory = new Mock<IRabbitMqQueueConsumerFactory>(MockBehavior.Strict);
        var fixture = CreateFixture(
            consumerFactory,
            new RabbitMqConsumerOptions { EndpointName = "empty-service" });

        await fixture.Service.StartAsync(CancellationToken.None);
        await fixture.Service.StopAsync(CancellationToken.None);

        consumerFactory.VerifyNoOtherCalls();
        fixture.VerifyTopology();
    }

    [Fact]
    public async Task StartAsync_WhenRetryTopologyDeclarationFails_DoesNotStartConsumers()
    {
        var consumerFactory =
            new Mock<IRabbitMqQueueConsumerFactory>(MockBehavior.Strict);
        var topologyException =
            new InvalidOperationException("retry topology declaration failed");
        var fixture = CreateFixture(
            consumerFactory,
            new RabbitMqConsumerOptions { EndpointName = "worker-service" },
            eventTypes: [typeof(ClientCreatedEvent)],
            retryTopologyException: topologyException);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Service.StartAsync(CancellationToken.None));

        Assert.Same(topologyException, exception);
        consumerFactory.VerifyNoOtherCalls();
        fixture.VerifyTopology();
    }

    [Fact]
    public async Task StartAsync_WhenDeadLetterTopologyDeclarationFails_DoesNotDeclareRetryTopologyOrStartConsumers()
    {
        var consumerFactory =
            new Mock<IRabbitMqQueueConsumerFactory>(MockBehavior.Strict);
        var topologyException = new InvalidOperationException(
            "dead-letter topology declaration failed");
        var fixture = CreateFixture(
            consumerFactory,
            new RabbitMqConsumerOptions { EndpointName = "worker-service" },
            eventTypes: [typeof(ClientCreatedEvent)],
            deadLetterTopologyException: topologyException);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Service.StartAsync(CancellationToken.None));

        Assert.Same(topologyException, exception);
        consumerFactory.VerifyNoOtherCalls();
        fixture.RetryTopologyCreator.VerifyNoOtherCalls();
        fixture.VerifyTopology();
    }

    [Fact]
    public async Task StartAsync_WhenLaterConsumerFails_DisposesPreviouslyStartedConsumers()
    {
        var options = new RabbitMqConsumerOptions
        {
            EndpointName = "worker-service",
            DefaultConsumerCount = 3
        };
        var disposeOrder = new List<string>();
        var startedConsumer = new TrackingConsumer("first", disposeOrder);
        var startException = new InvalidOperationException("consumer start failed");
        var consumerFactory = new Mock<IRabbitMqQueueConsumerFactory>(MockBehavior.Strict);
        consumerFactory
            .SetupSequence(x => x.StartAsync(
                It.IsAny<RabbitMqDeclaredSubscription>(),
                CancellationToken.None))
            .ReturnsAsync(startedConsumer)
            .ThrowsAsync(startException);
        var fixture = CreateFixture(
            consumerFactory,
            options,
            eventTypes: [typeof(ClientCreatedEvent)]);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Service.StartAsync(CancellationToken.None));

        Assert.Same(startException, exception);
        Assert.Equal(["first"], disposeOrder);
        Assert.Equal(1, startedConsumer.DisposeCalls);
        consumerFactory.Verify(
            x => x.StartAsync(
                It.IsAny<RabbitMqDeclaredSubscription>(),
                CancellationToken.None),
            Times.Exactly(2));

        await fixture.Service.StopAsync(CancellationToken.None);

        Assert.Equal(1, startedConsumer.DisposeCalls);
        fixture.VerifyTopology();
    }

    [Fact]
    public async Task StartAsync_WhenStartAndCleanupFail_AggregatesBothFailures()
    {
        var options = new RabbitMqConsumerOptions
        {
            EndpointName = "worker-service",
            DefaultConsumerCount = 2
        };
        var disposeOrder = new List<string>();
        var disposeException = new InvalidOperationException("dispose failed");
        var startedConsumer = new TrackingConsumer(
            "first",
            disposeOrder,
            disposeException);
        var startException = new InvalidOperationException("consumer start failed");
        var consumerFactory = new Mock<IRabbitMqQueueConsumerFactory>(MockBehavior.Strict);
        consumerFactory
            .SetupSequence(x => x.StartAsync(
                It.IsAny<RabbitMqDeclaredSubscription>(),
                CancellationToken.None))
            .ReturnsAsync(startedConsumer)
            .ThrowsAsync(startException);
        var fixture = CreateFixture(
            consumerFactory,
            options,
            eventTypes: [typeof(ClientCreatedEvent)]);

        var exception = await Assert.ThrowsAsync<AggregateException>(
            () => fixture.Service.StartAsync(CancellationToken.None));
        var failures = exception.Flatten().InnerExceptions;

        Assert.Contains(startException, failures);
        Assert.Contains(disposeException, failures);
        Assert.Equal(["first"], disposeOrder);
        Assert.Equal(1, startedConsumer.DisposeCalls);
        fixture.VerifyTopology();
    }

    [Fact]
    public async Task StopAsync_WhenConsumersFailToDispose_AttemptsAllAndClearsHandles()
    {
        var options = new RabbitMqConsumerOptions
        {
            EndpointName = "worker-service",
            DefaultConsumerCount = 3
        };
        var disposeOrder = new List<string>();
        var first = new TrackingConsumer("first", disposeOrder);
        var secondFailure = new InvalidOperationException("second failed");
        var second = new TrackingConsumer("second", disposeOrder, secondFailure);
        var thirdFailure = new InvalidOperationException("third failed");
        var third = new TrackingConsumer("third", disposeOrder, thirdFailure);
        var availableConsumers = new System.Collections.Generic.Queue<IAsyncDisposable>(
            [first, second, third]);
        var consumerFactory = new Mock<IRabbitMqQueueConsumerFactory>(MockBehavior.Strict);
        consumerFactory
            .Setup(x => x.StartAsync(
                It.IsAny<RabbitMqDeclaredSubscription>(),
                CancellationToken.None))
            .Returns(() => Task.FromResult(availableConsumers.Dequeue()));
        var fixture = CreateFixture(
            consumerFactory,
            options,
            eventTypes: [typeof(ClientCreatedEvent)]);
        await fixture.Service.StartAsync(CancellationToken.None);

        var exception = await Assert.ThrowsAsync<AggregateException>(
            () => fixture.Service.StopAsync(CancellationToken.None));
        var failures = exception.Flatten().InnerExceptions;

        Assert.Equal(["third", "second", "first"], disposeOrder);
        Assert.Contains(secondFailure, failures);
        Assert.Contains(thirdFailure, failures);
        Assert.Equal(1, first.DisposeCalls);
        Assert.Equal(1, second.DisposeCalls);
        Assert.Equal(1, third.DisposeCalls);

        await fixture.Service.StopAsync(CancellationToken.None);

        Assert.Equal(1, first.DisposeCalls);
        Assert.Equal(1, second.DisposeCalls);
        Assert.Equal(1, third.DisposeCalls);
        fixture.VerifyTopology();
    }

    private static HostedServiceFixture CreateFixture(Mock<IRabbitMqQueueConsumerFactory> consumerFactory, RabbitMqConsumerOptions options,
        IReadOnlyCollection<Type>? eventTypes = null, IReadOnlyCollection<Type>? commandTypes = null,
        Exception? retryTopologyException = null,
        Exception? deadLetterTopologyException = null)
    {
        var catalog = new Mock<IMessageHandlerCatalog>(MockBehavior.Strict);
        catalog
            .SetupGet(x => x.EventTypes)
            .Returns(eventTypes ?? Array.Empty<Type>());
        catalog
            .SetupGet(x => x.CommandTypes)
            .Returns(commandTypes ?? Array.Empty<Type>());
        var resolver = new RabbitMqSubscriptionResolver(
            catalog.Object,
            new MessageRouteResolver(),
            Options.Create(options));
        var subscriptions = resolver.Resolve();
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy = new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);

        foreach (var subscription in subscriptions)
        {
            var exchange = CreateExchange(subscription.ExchangeName);
            var queue = CreateQueue(subscription.QueueName);
            exchangeDeclareStrategy
                .Setup(x => x.DeclareExchangeAsync(
                    exchange.Name,
                    ExchangeType.Topic,
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(exchange);
            advancedBus
                .Setup(x => x.QueueDeclareAsync(
                    queue.Name,
                    It.IsAny<Action<IQueueDeclareConfiguration>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(queue);

            foreach (var binding in subscription.Bindings)
            {
                advancedBus
                    .Setup(x => x.QueueBindAsync(
                        queue.Name,
                        exchange.Name,
                        binding.RoutingKey,
                        null,
                        It.IsAny<CancellationToken>()))
                    .Returns(Task.CompletedTask);
            }
        }

        var topologyCreator = new RabbitMqTopologyCreator(
            advancedBus.Object,
            exchangeDeclareStrategy.Object,
            resolver);
        var retryTopologyCreator =
            new Mock<IRabbitMqRetryTopologyCreator>(MockBehavior.Strict);
        var deadLetterTopologyCreator =
            new Mock<IRabbitMqDeadLetterTopologyCreator>(
                MockBehavior.Strict);

        if (deadLetterTopologyException is null)
        {
            deadLetterTopologyCreator
                .Setup(x => x.DeclareAsync(
                    It.Is<IReadOnlyCollection<
                        RabbitMqDeclaredSubscription>>(declared =>
                        declared.Count == subscriptions.Count),
                    CancellationToken.None))
                .Returns(Task.CompletedTask);
        }
        else
        {
            deadLetterTopologyCreator
                .Setup(x => x.DeclareAsync(
                    It.IsAny<IReadOnlyCollection<
                        RabbitMqDeclaredSubscription>>(),
                    CancellationToken.None))
                .ThrowsAsync(deadLetterTopologyException);
        }

        if (deadLetterTopologyException is null &&
            retryTopologyException is null)
        {
            retryTopologyCreator
                .Setup(x => x.DeclareAsync(CancellationToken.None))
                .Returns(Task.CompletedTask);
        }
        else if (deadLetterTopologyException is null &&
            retryTopologyException is not null)
        {
            retryTopologyCreator
                .Setup(x => x.DeclareAsync(CancellationToken.None))
                .ThrowsAsync(retryTopologyException);
        }

        var service = new RabbitMqConsumerHostedService(
            topologyCreator,
            retryTopologyCreator.Object,
            deadLetterTopologyCreator.Object,
            consumerFactory.Object);

        return new HostedServiceFixture(
            service,
            advancedBus,
            exchangeDeclareStrategy,
            retryTopologyCreator,
            deadLetterTopologyCreator);
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

    private static RabbitQueue CreateQueue(string name)
    {
        return new RabbitQueue(
            name,
            true,
            false,
            false,
            new Dictionary<string, object>());
    }

    private sealed record HostedServiceFixture(
        RabbitMqConsumerHostedService Service,
        Mock<IAdvancedBus> AdvancedBus,
        Mock<IExchangeDeclareStrategy> ExchangeDeclareStrategy,
        Mock<IRabbitMqRetryTopologyCreator> RetryTopologyCreator,
        Mock<IRabbitMqDeadLetterTopologyCreator> DeadLetterTopologyCreator)
    {
        public void VerifyTopology()
        {
            AdvancedBus.VerifyAll();
            ExchangeDeclareStrategy.VerifyAll();
            RetryTopologyCreator.VerifyAll();
            DeadLetterTopologyCreator.VerifyAll();
        }
    }

    private sealed class TrackingConsumer(string name, ICollection<string> disposeOrder, Exception? disposeException = null) : IAsyncDisposable
    {
        public int DisposeCalls { get; private set; }

        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            disposeOrder.Add(name);

            return disposeException is null
                ? ValueTask.CompletedTask
                : new ValueTask(Task.FromException(disposeException));
        }
    }
}
