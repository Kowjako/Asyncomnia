using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using EasyNetQ;
using EasyNetQ.Consumer;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.DeadLettering;

public sealed class RabbitMqConsumeErrorStrategyTests
{
    [Fact]
    public async Task HandleErrorAsync_WhenRetryIsScheduled_ReturnsAckWithoutDeadLettering()
    {
        var context = CreateContext();
        var deadLetterPublisher =
            new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        var delayedRetryPublisher =
            new Mock<IRabbitMqDelayedRetryPublisher>(MockBehavior.Strict);
        delayedRetryPublisher
            .Setup(x => x.TryPublishAsync(context, CancellationToken.None))
            .ReturnsAsync(true);
        var strategy = CreateStrategy(
            deadLetterPublisher,
            delayedRetryPublisher,
            true);

        var ackStrategy = await strategy.HandleErrorAsync(
            context,
            new InvalidOperationException("handler failed"));

        Assert.Same(AckStrategies.AckAsync, ackStrategy);
        delayedRetryPublisher.VerifyAll();
        deadLetterPublisher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task HandleErrorAsync_WhenRetryPublishFails_ReturnsNackWithRequeueWithoutDeadLettering()
    {
        var context = CreateContext();
        var deadLetterPublisher =
            new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        var delayedRetryPublisher =
            new Mock<IRabbitMqDelayedRetryPublisher>(MockBehavior.Strict);
        delayedRetryPublisher
            .Setup(x => x.TryPublishAsync(context, CancellationToken.None))
            .ThrowsAsync(new InvalidOperationException("broker unavailable"));
        var strategy = CreateStrategy(
            deadLetterPublisher,
            delayedRetryPublisher,
            true);

        var ackStrategy = await strategy.HandleErrorAsync(
            context,
            new InvalidOperationException("handler failed"));

        Assert.Same(AckStrategies.NackWithRequeueAsync, ackStrategy);
        delayedRetryPublisher.VerifyAll();
        deadLetterPublisher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task HandleErrorAsync_WhenRetryIsNotScheduledAndDeadLetterPublishSucceeds_ReturnsAck()
    {
        var context = CreateContext();
        var deadLetterPublisher =
            new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        var delayedRetryPublisher =
            new Mock<IRabbitMqDelayedRetryPublisher>(MockBehavior.Strict);
        delayedRetryPublisher
            .Setup(x => x.TryPublishAsync(context, CancellationToken.None))
            .ReturnsAsync(false);
        deadLetterPublisher
            .Setup(x => x.PublishAsync(context, CancellationToken.None))
            .Returns(Task.CompletedTask);
        var strategy = CreateStrategy(
            deadLetterPublisher,
            delayedRetryPublisher,
            true);

        var ackStrategy = await strategy.HandleErrorAsync(
            context,
            new InvalidOperationException("handler failed"));

        Assert.Same(AckStrategies.AckAsync, ackStrategy);
        delayedRetryPublisher.VerifyAll();
        deadLetterPublisher.VerifyAll();
    }

    [Fact]
    public async Task HandleErrorAsync_WhenDeadLetteringIsDisabled_ReturnsNackWithoutRequeueWithoutPublishing()
    {
        var context = CreateContext();
        var deadLetterPublisher =
            new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        var strategy = CreateStrategy(deadLetterPublisher, false);

        var ackStrategy = await strategy.HandleErrorAsync(
            context,
            new InvalidOperationException("handler failed"));

        Assert.Same(AckStrategies.NackWithoutRequeueAsync, ackStrategy);
        deadLetterPublisher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task HandleErrorAsync_WhenDeadLetterPublishFails_ReturnsNackWithRequeue()
    {
        var context = CreateContext();
        var deadLetterPublisher =
            new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        deadLetterPublisher
            .Setup(x => x.PublishAsync(context, CancellationToken.None))
            .ThrowsAsync(new InvalidOperationException("broker unavailable"));
        var strategy = CreateStrategy(deadLetterPublisher, true);

        var ackStrategy = await strategy.HandleErrorAsync(
            context,
            new InvalidOperationException("handler failed"));

        Assert.Same(AckStrategies.NackWithRequeueAsync, ackStrategy);
        deadLetterPublisher.VerifyAll();
    }

    [Fact]
    public async Task HandleErrorAsync_WhenDeadLetterPublishIsCancelled_ReturnsNackWithRequeue()
    {
        var context = CreateContext();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();
        var deadLetterPublisher =
            new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        deadLetterPublisher
            .Setup(x => x.PublishAsync(
                context,
                cancellationTokenSource.Token))
            .ThrowsAsync(
                new OperationCanceledException(
                    cancellationTokenSource.Token));
        var strategy = CreateStrategy(deadLetterPublisher, true);

        var ackStrategy = await strategy.HandleErrorAsync(
            context,
            new InvalidOperationException("handler failed"),
            cancellationTokenSource.Token);

        Assert.Same(AckStrategies.NackWithRequeueAsync, ackStrategy);
        deadLetterPublisher.VerifyAll();
    }

    [Fact]
    public async Task HandleCancelledAsync_ReturnsNackWithRequeueWithoutPublishing()
    {
        var deadLetterPublisher =
            new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        var strategy = CreateStrategy(deadLetterPublisher, true);

        var ackStrategy = await strategy.HandleCancelledAsync(
            CreateContext());

        Assert.Same(AckStrategies.NackWithRequeueAsync, ackStrategy);
        deadLetterPublisher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task HandleErrorAsync_WhenExceptionIsNull_Throws()
    {
        var deadLetterPublisher =
            new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        var strategy = CreateStrategy(deadLetterPublisher, true);

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await strategy.HandleErrorAsync(CreateContext(), null!));

        deadLetterPublisher.VerifyNoOtherCalls();
    }

    private static RabbitMqConsumeErrorStrategy CreateStrategy(Mock<IRabbitMqDeadLetterPublisher> deadLetterPublisher, bool deadLetteringEnabled)
    {
        var delayedRetryPublisher =
            new Mock<IRabbitMqDelayedRetryPublisher>(MockBehavior.Strict);
        delayedRetryPublisher
            .Setup(x => x.TryPublishAsync(
                It.IsAny<ConsumeContext>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        return CreateStrategy(
            deadLetterPublisher,
            delayedRetryPublisher,
            deadLetteringEnabled);
    }

    private static RabbitMqConsumeErrorStrategy CreateStrategy(
        Mock<IRabbitMqDeadLetterPublisher> deadLetterPublisher,
        Mock<IRabbitMqDelayedRetryPublisher> delayedRetryPublisher,
        bool deadLetteringEnabled)
    {
        return new RabbitMqConsumeErrorStrategy(
            deadLetterPublisher.Object,
            delayedRetryPublisher.Object,
            Options.Create(new RabbitMqConsumerOptions
            {
                DeadLetteringEnabled = deadLetteringEnabled
            }),
            NullLogger<RabbitMqConsumeErrorStrategy>.Instance);
    }

    private static ConsumeContext CreateContext()
    {
        var receivedInfo = new MessageReceivedInfo(
            "consumer",
            1,
            false,
            "events/clients",
            "client-created-event",
            "events/clients/subscription/billing-service");
        var properties = new MessageProperties
        {
            MessageId = Guid.NewGuid().ToString("D")
        };

        ReadOnlyMemory<byte> body = new byte[] { 1, 2, 3 };
        var services = Mock.Of<IServiceProvider>();

        return new ConsumeContext(receivedInfo, properties, body, services, CancellationToken.None);
    }
}
