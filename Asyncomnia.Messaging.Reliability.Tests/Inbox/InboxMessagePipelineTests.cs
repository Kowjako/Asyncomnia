using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.Tests.TestDoubles;
using Moq;

namespace Asyncomnia.Messaging.Reliability.Tests.Inbox;

public sealed class InboxMessagePipelineTests
{
    [Fact]
    public async Task HandleEventAsync_WhenMessageIsNew_CommitsAfterHandlerCompletes()
    {
        var utcNow = new DateTimeOffset(2026, 11, 12, 11, 0, 0, TimeSpan.Zero);
        var message = new TestOutboxEvent("value");
        var context = CreateContext(message);
        var transaction = new Mock<IInboxTransaction>(MockBehavior.Strict);
        var writer = new Mock<IInboxWriter>(MockBehavior.Strict);
        var dispatcher = new Mock<IMessageHandlerDispatcher>(MockBehavior.Strict);
        var sequence = new MockSequence();
        InboxMessage? writtenMessage = null;
        using var cancellation = new CancellationTokenSource();

        transaction.InSequence(sequence)
            .Setup(x => x.BeginAsync(cancellation.Token))
            .Returns(Task.CompletedTask);
        writer.InSequence(sequence)
            .Setup(x => x.TryAddAsync(It.IsAny<InboxMessage>(), cancellation.Token))
            .Callback<InboxMessage, CancellationToken>((inboxMessage, _) =>
                writtenMessage = inboxMessage)
            .ReturnsAsync(true);
        dispatcher.InSequence(sequence)
            .Setup(x => x.DispatchEventAsync(
                It.Is<MessageEnvelope<TestOutboxEvent>>(envelope =>
                    ReferenceEquals(envelope.Message, message) &&
                    ReferenceEquals(envelope.Metadata, context.Metadata)),
                cancellation.Token))
            .Returns(Task.CompletedTask);
        transaction.InSequence(sequence)
            .Setup(x => x.CommitAsync(cancellation.Token))
            .Returns(Task.CompletedTask);

        var pipeline = CreatePipeline(transaction, writer, dispatcher, utcNow);

        await pipeline.HandleEventAsync<TestOutboxEvent>(context, cancellation.Token);

        var inboxMessage = Assert.IsType<InboxMessage>(writtenMessage);
        Assert.Equal(context.Metadata.MessageId, inboxMessage.MessageId);
        Assert.Equal(context.ConsumerId, inboxMessage.ConsumerId);
        Assert.Equal(context.ContractType, inboxMessage.ContractType);
        Assert.Equal(utcNow, inboxMessage.ReceivedAt);
        transaction.Verify(x => x.RollbackAsync(It.IsAny<CancellationToken>()), Times.Never);
        transaction.VerifyAll();
        writer.VerifyAll();
        dispatcher.VerifyAll();
    }

    [Fact]
    public async Task HandleCommandAsync_WhenMessageIsNew_DispatchesCommandAndCommits()
    {
        var message = new TestOutboxCommand("value");
        var context = CreateContext(message);
        var transaction = new Mock<IInboxTransaction>(MockBehavior.Strict);
        var writer = new Mock<IInboxWriter>(MockBehavior.Strict);
        var dispatcher = new Mock<IMessageHandlerDispatcher>(MockBehavior.Strict);
        var sequence = new MockSequence();

        transaction.InSequence(sequence)
            .Setup(x => x.BeginAsync(CancellationToken.None))
            .Returns(Task.CompletedTask);
        writer.InSequence(sequence)
            .Setup(x => x.TryAddAsync(It.IsAny<InboxMessage>(), CancellationToken.None))
            .ReturnsAsync(true);
        dispatcher.InSequence(sequence)
            .Setup(x => x.DispatchCommandAsync(
                It.Is<MessageEnvelope<TestOutboxCommand>>(envelope =>
                    ReferenceEquals(envelope.Message, message)),
                CancellationToken.None))
            .Returns(Task.CompletedTask);
        transaction.InSequence(sequence)
            .Setup(x => x.CommitAsync(CancellationToken.None))
            .Returns(Task.CompletedTask);
        var pipeline = CreatePipeline(
            transaction,
            writer,
            dispatcher,
            DateTimeOffset.UtcNow);

        await pipeline.HandleCommandAsync<TestOutboxCommand>(context);

        transaction.Verify(x => x.RollbackAsync(It.IsAny<CancellationToken>()), Times.Never);
        transaction.VerifyAll();
        writer.VerifyAll();
        dispatcher.VerifyAll();
    }

    [Fact]
    public async Task HandleEventAsync_WhenMessageIsDuplicate_SkipsHandlerAndRollsBack()
    {
        var transaction = new Mock<IInboxTransaction>(MockBehavior.Strict);
        var writer = new Mock<IInboxWriter>(MockBehavior.Strict);
        var dispatcher = new Mock<IMessageHandlerDispatcher>(MockBehavior.Strict);
        var sequence = new MockSequence();

        transaction.InSequence(sequence)
            .Setup(x => x.BeginAsync(CancellationToken.None))
            .Returns(Task.CompletedTask);
        writer.InSequence(sequence)
            .Setup(x => x.TryAddAsync(It.IsAny<InboxMessage>(), CancellationToken.None))
            .ReturnsAsync(false);
        transaction.InSequence(sequence)
            .Setup(x => x.RollbackAsync(CancellationToken.None))
            .Returns(Task.CompletedTask);
        var pipeline = CreatePipeline(
            transaction,
            writer,
            dispatcher,
            DateTimeOffset.UtcNow);

        await pipeline.HandleEventAsync<TestOutboxEvent>(
            CreateContext(new TestOutboxEvent("duplicate")));

        transaction.Verify(x => x.CommitAsync(It.IsAny<CancellationToken>()), Times.Never);
        dispatcher.VerifyNoOtherCalls();
        transaction.VerifyAll();
        writer.VerifyAll();
    }

    [Fact]
    public async Task HandleEventAsync_WhenWriterThrows_RollsBackAndPropagatesException()
    {
        var expectedException = new InvalidOperationException("Inbox write failed");
        var transaction = new Mock<IInboxTransaction>(MockBehavior.Strict);
        var writer = new Mock<IInboxWriter>(MockBehavior.Strict);
        var dispatcher = new Mock<IMessageHandlerDispatcher>(MockBehavior.Strict);
        var sequence = new MockSequence();

        transaction.InSequence(sequence)
            .Setup(x => x.BeginAsync(CancellationToken.None))
            .Returns(Task.CompletedTask);
        writer.InSequence(sequence)
            .Setup(x => x.TryAddAsync(It.IsAny<InboxMessage>(), CancellationToken.None))
            .ThrowsAsync(expectedException);
        transaction.InSequence(sequence)
            .Setup(x => x.RollbackAsync(CancellationToken.None))
            .Returns(Task.CompletedTask);
        var pipeline = CreatePipeline(
            transaction,
            writer,
            dispatcher,
            DateTimeOffset.UtcNow);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            pipeline.HandleEventAsync<TestOutboxEvent>(
                CreateContext(new TestOutboxEvent("value"))));

        Assert.Same(expectedException, exception);
        dispatcher.VerifyNoOtherCalls();
        transaction.VerifyAll();
        writer.VerifyAll();
    }

    [Fact]
    public async Task HandleEventAsync_WhenHandlerThrows_RollsBackAndPropagatesException()
    {
        var expectedException = new InvalidOperationException("Handler failed");
        var transaction = new Mock<IInboxTransaction>(MockBehavior.Strict);
        var writer = new Mock<IInboxWriter>(MockBehavior.Strict);
        var dispatcher = new Mock<IMessageHandlerDispatcher>(MockBehavior.Strict);
        var sequence = new MockSequence();

        transaction.InSequence(sequence)
            .Setup(x => x.BeginAsync(CancellationToken.None))
            .Returns(Task.CompletedTask);
        writer.InSequence(sequence)
            .Setup(x => x.TryAddAsync(It.IsAny<InboxMessage>(), CancellationToken.None))
            .ReturnsAsync(true);
        dispatcher.InSequence(sequence)
            .Setup(x => x.DispatchEventAsync(
                It.IsAny<MessageEnvelope<TestOutboxEvent>>(),
                CancellationToken.None))
            .ThrowsAsync(expectedException);
        transaction.InSequence(sequence)
            .Setup(x => x.RollbackAsync(CancellationToken.None))
            .Returns(Task.CompletedTask);
        var pipeline = CreatePipeline(
            transaction,
            writer,
            dispatcher,
            DateTimeOffset.UtcNow);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            pipeline.HandleEventAsync<TestOutboxEvent>(
                CreateContext(new TestOutboxEvent("value"))));

        Assert.Same(expectedException, exception);
        transaction.Verify(x => x.CommitAsync(It.IsAny<CancellationToken>()), Times.Never);
        transaction.VerifyAll();
        writer.VerifyAll();
        dispatcher.VerifyAll();
    }

    [Fact]
    public async Task HandleEventAsync_WhenCommitThrows_RollsBackAndPropagatesException()
    {
        var expectedException = new InvalidOperationException("Commit failed");
        var transaction = new Mock<IInboxTransaction>(MockBehavior.Strict);
        var writer = new Mock<IInboxWriter>(MockBehavior.Strict);
        var dispatcher = new Mock<IMessageHandlerDispatcher>(MockBehavior.Strict);
        var sequence = new MockSequence();

        transaction.InSequence(sequence)
            .Setup(x => x.BeginAsync(CancellationToken.None))
            .Returns(Task.CompletedTask);
        writer.InSequence(sequence)
            .Setup(x => x.TryAddAsync(It.IsAny<InboxMessage>(), CancellationToken.None))
            .ReturnsAsync(true);
        dispatcher.InSequence(sequence)
            .Setup(x => x.DispatchEventAsync(
                It.IsAny<MessageEnvelope<TestOutboxEvent>>(),
                CancellationToken.None))
            .Returns(Task.CompletedTask);
        transaction.InSequence(sequence)
            .Setup(x => x.CommitAsync(CancellationToken.None))
            .ThrowsAsync(expectedException);
        transaction.InSequence(sequence)
            .Setup(x => x.RollbackAsync(CancellationToken.None))
            .Returns(Task.CompletedTask);
        var pipeline = CreatePipeline(
            transaction,
            writer,
            dispatcher,
            DateTimeOffset.UtcNow);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            pipeline.HandleEventAsync<TestOutboxEvent>(
                CreateContext(new TestOutboxEvent("value"))));

        Assert.Same(expectedException, exception);
        transaction.VerifyAll();
        writer.VerifyAll();
        dispatcher.VerifyAll();
    }

    [Fact]
    public async Task HandleEventAsync_WhenContextIsNull_DoesNotBeginTransaction()
    {
        var transaction = new Mock<IInboxTransaction>(MockBehavior.Strict);
        var writer = new Mock<IInboxWriter>(MockBehavior.Strict);
        var dispatcher = new Mock<IMessageHandlerDispatcher>(MockBehavior.Strict);
        var pipeline = CreatePipeline(
            transaction,
            writer,
            dispatcher,
            DateTimeOffset.UtcNow);

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            pipeline.HandleEventAsync<TestOutboxEvent>(null!));

        transaction.VerifyNoOtherCalls();
        writer.VerifyNoOtherCalls();
        dispatcher.VerifyNoOtherCalls();
    }

    [Fact]
    public void Constructor_WithNullDependency_ThrowsArgumentNullException()
    {
        var transaction = new Mock<IInboxTransaction>().Object;
        var writer = new Mock<IInboxWriter>().Object;
        var dispatcher = new Mock<IMessageHandlerDispatcher>().Object;
        var next = new HandlerMessagePipeline(dispatcher);
        var timeProvider = new FixedTimeProvider(DateTimeOffset.UtcNow);

        Assert.Throws<ArgumentNullException>(() =>
            new InboxMessagePipeline(null!, writer, transaction, timeProvider));
        Assert.Throws<ArgumentNullException>(() =>
            new InboxMessagePipeline(next, null!, transaction, timeProvider));
        Assert.Throws<ArgumentNullException>(() =>
            new InboxMessagePipeline(next, writer, null!, timeProvider));
        Assert.Throws<ArgumentNullException>(() =>
            new InboxMessagePipeline(next, writer, transaction, null!));
    }

    private static InboxMessagePipeline CreatePipeline(
        Mock<IInboxTransaction> transaction,
        Mock<IInboxWriter> writer,
        Mock<IMessageHandlerDispatcher> dispatcher,
        DateTimeOffset utcNow)
    {
        return new InboxMessagePipeline(
            new HandlerMessagePipeline(dispatcher.Object),
            writer.Object,
            transaction.Object,
            new FixedTimeProvider(utcNow));
    }

    private static InboundMessageContext CreateContext(IMessage message)
    {
        return new InboundMessageContext(
            "events/test/subscription/test-service",
            $"{message.GetType().FullName}, {message.GetType().Assembly.GetName().Name}",
            message,
            new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow));
    }
}
