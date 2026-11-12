using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Routing;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer;

public sealed class RabbitMqConsumerHostedServiceTests
{
    [Fact]
    public async Task StartAsync_StartsOneConsumerForEachSubscription()
    {
        var options = new RabbitMqConsumerOptions
        {
            EndpointName = "worker-service"
        };
        var consumerFactory = new Mock<IRabbitMqQueueConsumerFactory>(MockBehavior.Strict);
        var disposeOrder = new List<string>();
        var consumers = new System.Collections.Generic.Queue<IAsyncDisposable>(
        [
            new TrackingConsumer("command", disposeOrder),
            new TrackingConsumer("event", disposeOrder)
        ]);
        var startedExchanges = new List<string>();
        consumerFactory
            .Setup(x => x.StartAsync(
                It.IsAny<RabbitMqSubscription>(),
                CancellationToken.None))
            .Callback<RabbitMqSubscription, CancellationToken>(
                (subscription, _) =>
                    startedExchanges.Add(subscription.ExchangeName))
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
                "events/clients"
            ],
            startedExchanges);
        consumerFactory.Verify(
            x => x.StartAsync(
                It.IsAny<RabbitMqSubscription>(),
                CancellationToken.None),
            Times.Exactly(2));

        await fixture.Service.StopAsync(CancellationToken.None);

        Assert.Equal(["event", "command"], disposeOrder);
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
            EndpointName = "worker-service"
        };
        var disposeOrder = new List<string>();
        var startedConsumer = new TrackingConsumer("first", disposeOrder);
        var startException = new InvalidOperationException("consumer start failed");
        var consumerFactory = new Mock<IRabbitMqQueueConsumerFactory>(MockBehavior.Strict);
        consumerFactory
            .SetupSequence(x => x.StartAsync(
                It.IsAny<RabbitMqSubscription>(),
                CancellationToken.None))
            .ReturnsAsync(startedConsumer)
            .ThrowsAsync(startException);
        var fixture = CreateFixture(
            consumerFactory,
            options,
            eventTypes: [typeof(ClientCreatedEvent)],
            commandTypes: [typeof(StartLongWorkJobCommand)]);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Service.StartAsync(CancellationToken.None));

        Assert.Same(startException, exception);
        Assert.Equal(["first"], disposeOrder);
        Assert.Equal(1, startedConsumer.DisposeCalls);
        consumerFactory.Verify(
            x => x.StartAsync(
                It.IsAny<RabbitMqSubscription>(),
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
            EndpointName = "worker-service"
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
                It.IsAny<RabbitMqSubscription>(),
                CancellationToken.None))
            .ReturnsAsync(startedConsumer)
            .ThrowsAsync(startException);
        var fixture = CreateFixture(
            consumerFactory,
            options,
            eventTypes: [typeof(ClientCreatedEvent)],
            commandTypes: [typeof(StartLongWorkJobCommand)]);

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
            EndpointName = "worker-service"
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
                It.IsAny<RabbitMqSubscription>(),
                CancellationToken.None))
            .Returns(() => Task.FromResult(availableConsumers.Dequeue()));
        var fixture = CreateFixture(
            consumerFactory,
            options,
            eventTypes:
            [
                typeof(ClientCreatedEvent),
                typeof(HTTPResponseReceivedEvent)
            ],
            commandTypes: [typeof(StartLongWorkJobCommand)]);
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
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);

        if (subscriptions.Count > 0)
        {
            channelFactory
                .Setup(x => x.CreateAsync(
                    null,
                    CancellationToken.None))
                .ReturnsAsync(channel.Object);
            channel
                .Setup(x => x.DisposeAsync())
                .Returns(ValueTask.CompletedTask);
        }

        foreach (var subscription in subscriptions)
        {
            channel
                .Setup(x => x.ExchangeDeclareAsync(
                    subscription.ExchangeName,
                    ExchangeType.Topic,
                    true,
                    false,
                    null,
                    false,
                    false,
                    CancellationToken.None))
                .Returns(Task.CompletedTask);
            channel
                .Setup(x => x.QueueDeclareAsync(
                    subscription.QueueName,
                    true,
                    false,
                    false,
                    null,
                    false,
                    false,
                    CancellationToken.None))
                .ReturnsAsync(
                    new QueueDeclareOk(
                        subscription.QueueName,
                        0,
                        0));

            foreach (var binding in subscription.Bindings)
            {
                channel
                    .Setup(x => x.QueueBindAsync(
                        subscription.QueueName,
                        subscription.ExchangeName,
                        binding.RoutingKey,
                        null,
                        false,
                        CancellationToken.None))
                    .Returns(Task.CompletedTask);
            }
        }

        var topologyCreator = new RabbitMqTopologyCreator(
            channelFactory.Object,
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
                        RabbitMqSubscription>>(declared =>
                        declared.Count == subscriptions.Count),
                    CancellationToken.None))
                .Returns(Task.CompletedTask);
        }
        else
        {
            deadLetterTopologyCreator
                .Setup(x => x.DeclareAsync(
                    It.IsAny<IReadOnlyCollection<
                        RabbitMqSubscription>>(),
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
            channelFactory,
            channel,
            retryTopologyCreator,
            deadLetterTopologyCreator);
    }

    private sealed record HostedServiceFixture(
        RabbitMqConsumerHostedService Service,
        Mock<IRabbitMqChannelFactory> ChannelFactory,
        Mock<IChannel> Channel,
        Mock<IRabbitMqRetryTopologyCreator> RetryTopologyCreator,
        Mock<IRabbitMqDeadLetterTopologyCreator> DeadLetterTopologyCreator)
    {
        public void VerifyTopology()
        {
            ChannelFactory.VerifyAll();
            Channel.VerifyAll();
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
