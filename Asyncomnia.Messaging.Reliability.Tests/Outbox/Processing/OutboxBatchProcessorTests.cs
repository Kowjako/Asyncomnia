using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Asyncomnia.Messaging.Reliability.Outbox.Processing;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Asyncomnia.Messaging.Reliability.Tests.TestDoubles;
using Asyncomnia.Messaging.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Asyncomnia.Messaging.Reliability.Tests.Outbox.Processing;

public sealed class OutboxBatchProcessorTests
{
    [Fact]
    public async Task ProcessBatchAsync_WhenNoMessagesAreClaimed_ReturnsZero()
    {
        var options = new OutboxOptions();
        var store = new Mock<IOutboxStore>(MockBehavior.Strict);
        var transport = new Mock<IOutboundMessageTransport>(MockBehavior.Strict);
        store
            .Setup(x => x.ClaimAsync(options.BatchSize, options.LeaseDuration, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<ClaimedOutboxMessage>());
        var processor = CreateProcessor(store.Object, transport.Object, options);

        var processedCount = await processor.ProcessBatchAsync();

        Assert.Equal(0, processedCount);
        store.Verify(
            x => x.ClaimAsync(options.BatchSize, options.LeaseDuration, It.IsAny<CancellationToken>()),
            Times.Once);
        transport.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task ProcessBatchAsync_WithValidMessage_DeserializesDispatchesAndCompletesIt()
    {
        var options = new OutboxOptions();
        var claimedMessage = CreateClaimedMessage();
        var store = new Mock<IOutboxStore>(MockBehavior.Strict);
        var transport = new Mock<IOutboundMessageTransport>(MockBehavior.Strict);
        IMessage? dispatchedMessage = null;
        MessageMetadata? dispatchedMetadata = null;
        CancellationToken dispatchedToken = default;
        using var cancellationTokenSource = new CancellationTokenSource();
        store
            .Setup(x => x.ClaimAsync(options.BatchSize, options.LeaseDuration, cancellationTokenSource.Token))
            .ReturnsAsync([claimedMessage]);
        transport
            .Setup(x => x.DispatchAsync(It.IsAny<IMessage>(), It.IsAny<MessageMetadata>(), cancellationTokenSource.Token))
            .Callback<IMessage, MessageMetadata, CancellationToken>((message, metadata, token) =>
            {
                dispatchedMessage = message;
                dispatchedMetadata = metadata;
                dispatchedToken = token;
            })
            .Returns(Task.CompletedTask);
        store
            .Setup(x => x.CompleteAsync(
                claimedMessage.Message.MessageId,
                claimedMessage.LeaseId,
                cancellationTokenSource.Token))
            .ReturnsAsync(true);
        var processor = CreateProcessor(store.Object, transport.Object, options);

        var processedCount = await processor.ProcessBatchAsync(cancellationTokenSource.Token);

        Assert.Equal(1, processedCount);
        var @event = Assert.IsType<TestOutboxEvent>(dispatchedMessage);
        Assert.Equal("event-value", @event.Value);
        Assert.NotNull(dispatchedMetadata);
        Assert.Equal(claimedMessage.Message.MessageId, dispatchedMetadata.MessageId);
        Assert.Equal(claimedMessage.Message.CorrelationId, dispatchedMetadata.CorrelationId);
        Assert.Equal(claimedMessage.Message.CreatedAt, dispatchedMetadata.CreatedAt);
        Assert.Equal(cancellationTokenSource.Token, dispatchedToken);
        store.Verify(
            x => x.CompleteAsync(
                claimedMessage.Message.MessageId,
                claimedMessage.LeaseId,
                cancellationTokenSource.Token),
            Times.Once);
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenDispatchFailsBeforeFinalAttempt_SchedulesRetry()
    {
        var options = new OutboxOptions { RetryDelay = TimeSpan.FromSeconds(7) };
        var claimedMessage = CreateClaimedMessage(attemptNumber: 2);
        var store = CreateStoreReturning(options, claimedMessage);
        var transport = new Mock<IOutboundMessageTransport>(MockBehavior.Strict);
        transport
            .Setup(x => x.DispatchAsync(It.IsAny<IMessage>(), It.IsAny<MessageMetadata>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Transport failed"));
        store
            .Setup(x => x.ScheduleRetryAsync(
                claimedMessage.Message.MessageId,
                claimedMessage.LeaseId,
                "Transport failed",
                options.RetryDelay,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        var processor = CreateProcessor(store.Object, transport.Object, options);

        var processedCount = await processor.ProcessBatchAsync();

        Assert.Equal(1, processedCount);
        store.Verify(
            x => x.ScheduleRetryAsync(
                claimedMessage.Message.MessageId,
                claimedMessage.LeaseId,
                "Transport failed",
                options.RetryDelay,
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenDispatchFailsOnFinalAttempt_MarksMessageAsFailed()
    {
        var options = new OutboxOptions { MaxAttempts = 3 };
        var claimedMessage = CreateClaimedMessage(attemptNumber: options.MaxAttempts);
        var store = CreateStoreReturning(options, claimedMessage);
        var transport = new Mock<IOutboundMessageTransport>(MockBehavior.Strict);
        transport
            .Setup(x => x.DispatchAsync(It.IsAny<IMessage>(), It.IsAny<MessageMetadata>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Transport failed"));
        store
            .Setup(x => x.FailAsync(
                claimedMessage.Message.MessageId,
                claimedMessage.LeaseId,
                "Transport failed",
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        var processor = CreateProcessor(store.Object, transport.Object, options);

        await processor.ProcessBatchAsync();

        store.Verify(
            x => x.FailAsync(
                claimedMessage.Message.MessageId,
                claimedMessage.LeaseId,
                "Transport failed",
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenAttemptExceedsMaximum_FailsWithoutDispatching()
    {
        var options = new OutboxOptions { MaxAttempts = 3 };
        var claimedMessage = CreateClaimedMessage(attemptNumber: options.MaxAttempts + 1);
        var store = CreateStoreReturning(options, claimedMessage);
        var transport = new Mock<IOutboundMessageTransport>(MockBehavior.Strict);
        store
            .Setup(x => x.FailAsync(
                claimedMessage.Message.MessageId,
                claimedMessage.LeaseId,
                It.Is<string>(error => error.Contains("Maximum number of attempts exceeded")),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        var processor = CreateProcessor(store.Object, transport.Object, options);

        await processor.ProcessBatchAsync();

        transport.VerifyNoOtherCalls();
        store.Verify(
            x => x.FailAsync(
                claimedMessage.Message.MessageId,
                claimedMessage.LeaseId,
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenContractTypeCannotBeResolved_SchedulesRetry()
    {
        var options = new OutboxOptions();
        var claimedMessage = CreateClaimedMessage(contractType: "Missing.Message, Missing.Contracts");
        var store = CreateStoreReturning(options, claimedMessage);
        var transport = new Mock<IOutboundMessageTransport>(MockBehavior.Strict);
        store
            .Setup(x => x.ScheduleRetryAsync(
                claimedMessage.Message.MessageId,
                claimedMessage.LeaseId,
                It.Is<string>(error => error.Contains("Could not resolve message contract type")),
                options.RetryDelay,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        var processor = CreateProcessor(store.Object, transport.Object, options);

        await processor.ProcessBatchAsync();

        transport.VerifyNoOtherCalls();
        store.Verify(
            x => x.ScheduleRetryAsync(
                claimedMessage.Message.MessageId,
                claimedMessage.LeaseId,
                It.IsAny<string>(),
                options.RetryDelay,
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenMessageJsonIsInvalid_SchedulesRetryWithoutDispatching()
    {
        var options = new OutboxOptions();
        var claimedMessage = CreateClaimedMessage(messageJson: "{");
        var store = CreateStoreReturning(options, claimedMessage);
        var transport = new Mock<IOutboundMessageTransport>(MockBehavior.Strict);
        store
            .Setup(x => x.ScheduleRetryAsync(
                claimedMessage.Message.MessageId,
                claimedMessage.LeaseId,
                It.IsAny<string>(),
                options.RetryDelay,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        var processor = CreateProcessor(store.Object, transport.Object, options);

        await processor.ProcessBatchAsync();

        transport.VerifyNoOtherCalls();
        store.Verify(
            x => x.ScheduleRetryAsync(
                claimedMessage.Message.MessageId,
                claimedMessage.LeaseId,
                It.IsAny<string>(),
                options.RetryDelay,
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenCancellationIsRequested_PropagatesCancellationWithoutChangingState()
    {
        var options = new OutboxOptions();
        var claimedMessage = CreateClaimedMessage();
        var store = new Mock<IOutboxStore>(MockBehavior.Strict);
        var transport = new Mock<IOutboundMessageTransport>(MockBehavior.Strict);
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();
        store
            .Setup(x => x.ClaimAsync(options.BatchSize, options.LeaseDuration, cancellationTokenSource.Token))
            .ReturnsAsync([claimedMessage]);
        transport
            .Setup(x => x.DispatchAsync(It.IsAny<IMessage>(), It.IsAny<MessageMetadata>(), cancellationTokenSource.Token))
            .ThrowsAsync(new OperationCanceledException(cancellationTokenSource.Token));
        var processor = CreateProcessor(store.Object, transport.Object, options);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => processor.ProcessBatchAsync(cancellationTokenSource.Token));

        store.Verify(
            x => x.ClaimAsync(options.BatchSize, options.LeaseDuration, cancellationTokenSource.Token),
            Times.Once);
        store.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenCompletionLosesLease_LogsWarningWithoutSchedulingRetry()
    {
        var options = new OutboxOptions();
        var claimedMessage = CreateClaimedMessage();
        var store = CreateStoreReturning(options, claimedMessage);
        var transport = new Mock<IOutboundMessageTransport>(MockBehavior.Strict);
        var logger = new CapturingLogger<OutboxBatchProcessor>();
        transport
            .Setup(x => x.DispatchAsync(It.IsAny<IMessage>(), It.IsAny<MessageMetadata>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        store
            .Setup(x => x.CompleteAsync(
                claimedMessage.Message.MessageId,
                claimedMessage.LeaseId,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        var processor = CreateProcessor(store.Object, transport.Object, options, logger);

        await processor.ProcessBatchAsync();

        var entry = Assert.Single(logger.Entries);
        Assert.Equal(LogLevel.Warning, entry.LogLevel);
        Assert.Contains(claimedMessage.Message.MessageId.ToString(), entry.Message);
        Assert.Contains(claimedMessage.LeaseId.ToString(), entry.Message);
    }

    private static Mock<IOutboxStore> CreateStoreReturning(OutboxOptions options, ClaimedOutboxMessage claimedMessage)
    {
        var store = new Mock<IOutboxStore>(MockBehavior.Strict);
        store
            .Setup(x => x.ClaimAsync(options.BatchSize, options.LeaseDuration, It.IsAny<CancellationToken>()))
            .ReturnsAsync([claimedMessage]);

        return store;
    }

    private static OutboxBatchProcessor CreateProcessor(IOutboxStore store, IOutboundMessageTransport transport, OutboxOptions options, ILogger<OutboxBatchProcessor>? logger = null)
    {
        return new OutboxBatchProcessor(
            store,
            transport,
            new OutboxJsonSerializer(),
            Options.Create(options),
            logger ?? NullLogger<OutboxBatchProcessor>.Instance);
    }

    private static ClaimedOutboxMessage CreateClaimedMessage(int attemptNumber = 1, string? contractType = null, string messageJson = "{\"value\":\"event-value\"}")
    {
        var messageType = typeof(TestOutboxEvent);
        var outboxMessage = new OutboxMessage(
            Guid.NewGuid(),
            contractType ?? $"{messageType.FullName}, {messageType.Assembly.GetName().Name}",
            messageJson,
            Guid.NewGuid(),
            new DateTimeOffset(2026, 11, 5, 12, 0, 0, TimeSpan.Zero));

        return new ClaimedOutboxMessage(
            outboxMessage,
            Guid.NewGuid(),
            attemptNumber,
            DateTimeOffset.UtcNow.AddMinutes(1));
    }
}