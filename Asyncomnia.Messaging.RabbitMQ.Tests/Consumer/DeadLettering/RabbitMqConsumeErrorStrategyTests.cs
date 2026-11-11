using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.DeadLettering;

public sealed class RabbitMqConsumeErrorStrategyTests
{
    [Fact]
    public async Task HandleErrorAsync_WhenRetryIsScheduled_ReturnsAckWithoutDeadLettering()
    {
        var context = CreateContext();
        var deadLetterPublisher = new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        var delayedRetryPublisher = new Mock<IRabbitMqDelayedRetryPublisher>(MockBehavior.Strict);
        delayedRetryPublisher
            .Setup(x => x.TryPublishAsync(context, CancellationToken.None))
            .ReturnsAsync(true);
        var strategy = CreateStrategy(deadLetterPublisher, delayedRetryPublisher, true);

        var response = await strategy.HandleErrorAsync(
            context,
            new InvalidOperationException("handler failed"));

        Assert.Equal(AckResponse.Ack, response);
        delayedRetryPublisher.VerifyAll();
        deadLetterPublisher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task HandleErrorAsync_WhenRetryPublishFails_ReturnsNackWithRequeueWithoutDeadLettering()
    {
        var context = CreateContext();
        var deadLetterPublisher = new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        var delayedRetryPublisher = new Mock<IRabbitMqDelayedRetryPublisher>(MockBehavior.Strict);
        delayedRetryPublisher
            .Setup(x => x.TryPublishAsync(context, CancellationToken.None))
            .ThrowsAsync(new InvalidOperationException("broker unavailable"));
        var strategy = CreateStrategy(deadLetterPublisher, delayedRetryPublisher, true);

        var response = await strategy.HandleErrorAsync(
            context,
            new InvalidOperationException("handler failed"));

        Assert.Equal(AckResponse.NackWithRequeue, response);
        delayedRetryPublisher.VerifyAll();
        deadLetterPublisher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task HandleErrorAsync_WhenRetryIsNotScheduledAndDeadLetterPublishSucceeds_ReturnsAck()
    {
        var context = CreateContext();
        var deadLetterPublisher = new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        var delayedRetryPublisher = new Mock<IRabbitMqDelayedRetryPublisher>(MockBehavior.Strict);
        delayedRetryPublisher
            .Setup(x => x.TryPublishAsync(context, CancellationToken.None))
            .ReturnsAsync(false);
        deadLetterPublisher
            .Setup(x => x.PublishAsync(context, CancellationToken.None))
            .Returns(Task.CompletedTask);
        var strategy = CreateStrategy(deadLetterPublisher, delayedRetryPublisher, true);

        var response = await strategy.HandleErrorAsync(
            context,
            new InvalidOperationException("handler failed"));

        Assert.Equal(AckResponse.Ack, response);
        delayedRetryPublisher.VerifyAll();
        deadLetterPublisher.VerifyAll();
    }

    [Fact]
    public async Task HandleErrorAsync_WhenDeadLetteringIsDisabled_ReturnsNackWithoutRequeue()
    {
        var context = CreateContext();
        var deadLetterPublisher = new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        var delayedRetryPublisher = new Mock<IRabbitMqDelayedRetryPublisher>(MockBehavior.Strict);
        delayedRetryPublisher
            .Setup(x => x.TryPublishAsync(context, CancellationToken.None))
            .ReturnsAsync(false);
        var strategy = CreateStrategy(deadLetterPublisher, delayedRetryPublisher, false);

        var response = await strategy.HandleErrorAsync(
            context,
            new InvalidOperationException("handler failed"));

        Assert.Equal(AckResponse.NackWithoutRequeue, response);
        delayedRetryPublisher.VerifyAll();
        deadLetterPublisher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task HandleErrorAsync_WhenDeadLetterPublishFails_ReturnsNackWithRequeue()
    {
        var context = CreateContext();
        var deadLetterPublisher = new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        var delayedRetryPublisher = new Mock<IRabbitMqDelayedRetryPublisher>(MockBehavior.Strict);
        delayedRetryPublisher
            .Setup(x => x.TryPublishAsync(context, CancellationToken.None))
            .ReturnsAsync(false);
        deadLetterPublisher
            .Setup(x => x.PublishAsync(context, CancellationToken.None))
            .ThrowsAsync(new InvalidOperationException("broker unavailable"));
        var strategy = CreateStrategy(deadLetterPublisher, delayedRetryPublisher, true);

        var response = await strategy.HandleErrorAsync(
            context,
            new InvalidOperationException("handler failed"));

        Assert.Equal(AckResponse.NackWithRequeue, response);
        delayedRetryPublisher.VerifyAll();
        deadLetterPublisher.VerifyAll();
    }

    [Fact]
    public async Task HandleErrorAsync_WhenDeadLetterPublishIsCancelled_ReturnsNackWithRequeue()
    {
        var context = CreateContext();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();
        var deadLetterPublisher = new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        var delayedRetryPublisher = new Mock<IRabbitMqDelayedRetryPublisher>(MockBehavior.Strict);
        delayedRetryPublisher
            .Setup(x => x.TryPublishAsync(context, cancellationTokenSource.Token))
            .ReturnsAsync(false);
        deadLetterPublisher
            .Setup(x => x.PublishAsync(context, cancellationTokenSource.Token))
            .ThrowsAsync(new OperationCanceledException(cancellationTokenSource.Token));
        var strategy = CreateStrategy(deadLetterPublisher, delayedRetryPublisher, true);

        var response = await strategy.HandleErrorAsync(
            context,
            new InvalidOperationException("handler failed"),
            cancellationTokenSource.Token);

        Assert.Equal(AckResponse.NackWithRequeue, response);
        delayedRetryPublisher.VerifyAll();
        deadLetterPublisher.VerifyAll();
    }

    [Fact]
    public async Task HandleErrorAsync_WhenContextIsNull_Throws()
    {
        var strategy = CreateStrategy(
            new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict),
            new Mock<IRabbitMqDelayedRetryPublisher>(MockBehavior.Strict),
            true);

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await strategy.HandleErrorAsync(
                null!,
                new InvalidOperationException("handler failed")));
    }

    [Fact]
    public async Task HandleErrorAsync_WhenExceptionIsNull_Throws()
    {
        var strategy = CreateStrategy(
            new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict),
            new Mock<IRabbitMqDelayedRetryPublisher>(MockBehavior.Strict),
            true);

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await strategy.HandleErrorAsync(CreateContext(), null!));
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

    private static RabbitMqConsumeContext CreateContext()
    {
        return new RabbitMqConsumeContext(
            "events/clients/subscription/billing-service",
            "events/clients",
            "client-created-event",
            new BasicProperties
            {
                MessageId = Guid.NewGuid().ToString("D")
            },
            new byte[] { 1, 2, 3 });
    }
}
