using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Internal.Serialization;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer;

public sealed class RabbitMqQueueConsumerFactoryTests
{
    [Fact]
    public async Task StartAsync_AppliesSettingsAndStartsConsumerForSubscriptionQueue()
    {
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        IAsyncBasicConsumer? capturedConsumer = null;
        channel
            .Setup(x => x.BasicQosAsync(0, 3, false, CancellationToken.None))
            .Returns(Task.CompletedTask);
        channel
            .Setup(x => x.BasicConsumeAsync(
                "events/clients/subscription/test-service",
                false,
                string.Empty,
                false,
                false,
                null,
                It.IsAny<IAsyncBasicConsumer>(),
                CancellationToken.None))
            .Callback<string, bool, string, bool, bool, IDictionary<string, object?>?, IAsyncBasicConsumer, CancellationToken>(
                (_, _, _, _, _, _, consumer, _) => capturedConsumer = consumer)
            .ReturnsAsync("consumer-tag");
        var channelFactory = new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        CreateChannelOptions? capturedOptions = null;
        channelFactory
            .Setup(x => x.CreateAsync(
                It.IsAny<CreateChannelOptions>(),
                CancellationToken.None))
            .Callback<CreateChannelOptions?, CancellationToken>(
                (options, _) => capturedOptions = options)
            .ReturnsAsync(channel.Object);
        await using var dependencies = CreateFactory(channelFactory.Object);
        var subscription = CreateSubscription(new RabbitMqSubscriptionSettings(false, 3, 4));

        var result = await dependencies.Factory.StartAsync(subscription);

        Assert.Same(channel.Object, result);
        Assert.NotNull(capturedConsumer);
        Assert.Same(channel.Object, capturedConsumer!.Channel);
        Assert.NotNull(capturedOptions);
        Assert.False(capturedOptions!.PublisherConfirmationsEnabled);
        Assert.False(capturedOptions.PublisherConfirmationTrackingEnabled);
        Assert.Equal((ushort)4, capturedOptions.ConsumerDispatchConcurrency);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task StartAsync_WhenCancellationIsAlreadyRequested_DoesNotCreateChannel()
    {
        var channelFactory = new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        await using var dependencies = CreateFactory(channelFactory.Object);
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => dependencies.Factory.StartAsync(
                CreateSubscription(new RabbitMqSubscriptionSettings(false, 1, 1)),
                cancellationTokenSource.Token));

        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task StartAsync_WhenSubscriptionIsNull_DoesNotCreateChannel()
    {
        var channelFactory = new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        await using var dependencies = CreateFactory(channelFactory.Object);

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => dependencies.Factory.StartAsync(null!));

        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task StartAsync_WhenConsumerRegistrationFails_DisposesChannelAndRethrows()
    {
        var expectedException = new InvalidOperationException("consume failed");
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        channel
            .Setup(x => x.BasicQosAsync(0, 1, false, CancellationToken.None))
            .Returns(Task.CompletedTask);
        channel
            .Setup(x => x.BasicConsumeAsync(
                It.IsAny<string>(),
                false,
                string.Empty,
                false,
                false,
                null,
                It.IsAny<IAsyncBasicConsumer>(),
                CancellationToken.None))
            .ThrowsAsync(expectedException);
        channel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var channelFactory = new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        channelFactory
            .Setup(x => x.CreateAsync(
                It.IsAny<CreateChannelOptions>(),
                CancellationToken.None))
            .ReturnsAsync(channel.Object);
        await using var dependencies = CreateFactory(channelFactory.Object);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => dependencies.Factory.StartAsync(
                CreateSubscription(new RabbitMqSubscriptionSettings(false, 1, 1))));

        Assert.Same(expectedException, exception);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    private static FactoryDependencies CreateFactory(IRabbitMqChannelFactory channelFactory)
    {
        var services = new ServiceCollection();
        var provider = services.BuildServiceProvider();
        var deadLetterPublisher = new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        var delayedRetryPublisher = new Mock<IRabbitMqDelayedRetryPublisher>(MockBehavior.Strict);
        var errorStrategy = new RabbitMqConsumeErrorStrategy(
            deadLetterPublisher.Object,
            delayedRetryPublisher.Object,
            Options.Create(new RabbitMqConsumerOptions()),
            NullLogger<RabbitMqConsumeErrorStrategy>.Instance);
        var messageConsumer = new RabbitMqMessageConsumer(
            provider.GetRequiredService<IServiceScopeFactory>(),
            new MessageJsonSerializer(),
            new MessageDispatchCatalog([]),
            errorStrategy);

        return new FactoryDependencies(
            provider,
            new RabbitMqQueueConsumerFactory(channelFactory, messageConsumer));
    }

    private static RabbitMqSubscription CreateSubscription(RabbitMqSubscriptionSettings settings)
    {
        return new RabbitMqSubscription
        {
            ExchangeName = "events/clients",
            QueueName = "events/clients/subscription/test-service",
            Bindings = [],
            Settings = settings
        };
    }

    private sealed record FactoryDependencies(
        ServiceProvider Provider,
        RabbitMqQueueConsumerFactory Factory) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            return Provider.DisposeAsync();
        }
    }
}
