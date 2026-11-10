using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using EasyNetQ;
using EasyNetQ.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using RabbitQueue = EasyNetQ.Topology.Queue;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer;

public sealed class RabbitMqQueueConsumerFactoryTests
{
    [Fact]
    public async Task StartAsync_AppliesSettingsAndConfiguresSubscriptionQueue()
    {
        var configuration = new CapturingConsumeConfiguration();
        var consumer = new Mock<IAsyncDisposable>(MockBehavior.Strict);
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        advancedBus
            .Setup(x => x.ConsumeAsync(It.IsAny<Action<IConsumeConfiguration>>()))
            .Callback<Action<IConsumeConfiguration>>(configure => configure(configuration))
            .ReturnsAsync(consumer.Object);
        var factory = CreateFactory(advancedBus);
        var subscription = CreateSubscription(
            new RabbitMqConsumerSettings(2, 3, 4));

        var result = await factory.StartAsync(subscription);

        Assert.Same(consumer.Object, result);
        Assert.Equal((ushort)3, configuration.PrefetchCount);
        Assert.Equal((ushort)4, configuration.DispatcherConcurrency);
        Assert.True(configuration.Queue.HasValue);
        Assert.Equal(
            subscription.QueueName,
            configuration.Queue.Value.Name);
        Assert.NotNull(configuration.RegisterHandlers);
        Assert.NotNull(configuration.ConfigureQueue);

        var handlerRegistration = new Mock<IHandlerRegistration>(MockBehavior.Strict);
        handlerRegistration
            .SetupSet(x => x.ThrowOnNoMatchingHandler = true);
        configuration.RegisterHandlers!(handlerRegistration.Object);

        var queueConfiguration = new Mock<IPerQueueConsumeConfiguration>(MockBehavior.Strict);
        configuration.ConfigureQueue!(queueConfiguration.Object);

        handlerRegistration.VerifyAll();
        queueConfiguration.VerifyNoOtherCalls();
        advancedBus.VerifyAll();
    }

    [Fact]
    public async Task StartAsync_WhenCancellationIsAlreadyRequested_DoesNotCallEasyNetQ()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var factory = CreateFactory(advancedBus);
        var subscription = CreateSubscription(
            new RabbitMqConsumerSettings(1, 1, 1));
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => factory.StartAsync(subscription, cancellationTokenSource.Token));

        advancedBus.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task StartAsync_WhenSubscriptionIsNull_DoesNotCallEasyNetQ()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var factory = CreateFactory(advancedBus);

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => factory.StartAsync(null!));

        advancedBus.VerifyNoOtherCalls();
    }

    private static RabbitMqQueueConsumerFactory CreateFactory(Mock<IAdvancedBus> advancedBus)
    {
        var scopeFactory = new Mock<IServiceScopeFactory>(MockBehavior.Strict);
        var messageConsumer = new RabbitMqMessageConsumer(scopeFactory.Object);
        var registrar = new RabbitMqHandlerRegistrar(messageConsumer);

        return new RabbitMqQueueConsumerFactory(advancedBus.Object, registrar);
    }

    private static RabbitMqSubscription CreateSubscription(
        RabbitMqConsumerSettings settings)
    {
        return new RabbitMqSubscription
        {
            ExchangeName = "events/clients",
            QueueName = "events/clients/subscription/test-service",
            Bindings = [],
            ConsumerSettings = settings
        };
    }

    private sealed class CapturingConsumeConfiguration : IConsumeConfiguration
    {
        public ushort? PrefetchCount { get; private set; }

        public ushort? DispatcherConcurrency { get; private set; }

        public RabbitQueue? Queue { get; private set; }

        public Action<IHandlerRegistration>? RegisterHandlers { get; private set; }

        public Action<IPerQueueConsumeConfiguration>? ConfigureQueue { get; private set; }

        public IConsumeConfiguration WithPrefetchCount(ushort prefetchCount)
        {
            PrefetchCount = prefetchCount;
            return this;
        }

        public IConsumeConfiguration WithConsumerDispatcherConcurrency(ushort consumerDispatcherConcurrency)
        {
            DispatcherConcurrency = consumerDispatcherConcurrency;
            return this;
        }

        public IConsumeConfiguration ForQueue(in RabbitQueue queue, MessageHandler handler, Action<IPerQueueConsumeConfiguration> configure)
        {
            throw new InvalidOperationException("Raw handler overload was not expected.");
        }

        public IConsumeConfiguration ForQueue(in RabbitQueue queue, Action<IHandlerRegistration> handlers, Action<IPerQueueConsumeConfiguration> configure)
        {
            Queue = queue;
            RegisterHandlers = handlers;
            ConfigureQueue = configure;
            return this;
        }
    }
}
