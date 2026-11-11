using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Internal;
using Asyncomnia.Messaging.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Tests.Inbound;

public sealed class InboundMessageProcessorTests
{
    [Fact]
    public async Task ProcessAsync_WithRegisteredEvent_DispatchesMessageAndMetadataToPipeline()
    {
        var message = new TestEvent("event");
        var metadata = new MessageMetadata(
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);
        using var cancellationTokenSource =
            new CancellationTokenSource();
        var token = cancellationTokenSource.Token;
        var pipeline = new RecordingInboundMessagePipeline();
        var processor = CreateProcessor(
            pipeline,
            MessageDispatchDescriptorFactory.CreateEvent<TestEvent>());
        var context = new InboundMessageContext(
            "events/test/subscription/test-service",
            ClrMessageTypeResolver.GetContractType(typeof(TestEvent)),
            message,
            metadata);

        await processor.ProcessAsync(context, token);

        var envelope = Assert.IsType<MessageEnvelope<TestEvent>>(
            pipeline.EventEnvelope);
        Assert.Same(message, envelope.Message);
        Assert.Same(metadata, envelope.Metadata);
        Assert.Equal(token, pipeline.Token);
        Assert.Null(pipeline.CommandEnvelope);
    }

    [Fact]
    public async Task ProcessAsync_WithRegisteredCommand_DispatchesMessageAndMetadataToPipeline()
    {
        var message = new TestCommand("command");
        var metadata = new MessageMetadata(
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);
        var pipeline = new RecordingInboundMessagePipeline();
        var processor = CreateProcessor(
            pipeline,
            MessageDispatchDescriptorFactory.CreateCommand<TestCommand>());
        var context = new InboundMessageContext(
            "commands/test/subscription/test-service",
            ClrMessageTypeResolver.GetContractType(typeof(TestCommand)),
            message,
            metadata);

        await processor.ProcessAsync(context);

        var envelope = Assert.IsType<MessageEnvelope<TestCommand>>(
            pipeline.CommandEnvelope);
        Assert.Same(message, envelope.Message);
        Assert.Same(metadata, envelope.Metadata);
        Assert.Null(pipeline.EventEnvelope);
    }

    [Fact]
    public async Task ProcessAsync_WithUnknownContract_ThrowsWithoutInvokingPipeline()
    {
        var pipeline = new RecordingInboundMessagePipeline();
        var processor = CreateProcessor(
            pipeline,
            MessageDispatchDescriptorFactory.CreateEvent<TestEvent>());
        var context = new InboundMessageContext(
            "events/test/subscription/test-service",
            "Missing.Contract, Missing.Assembly",
            new TestEvent("event"),
            new MessageMetadata(
                Guid.NewGuid(),
                DateTimeOffset.UtcNow));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => processor.ProcessAsync(context));

        Assert.Equal(
            "No dispatch descriptor registered for contract " +
            "'Missing.Contract, Missing.Assembly'",
            exception.Message);
        Assert.Null(pipeline.EventEnvelope);
        Assert.Null(pipeline.CommandEnvelope);
    }

    [Fact]
    public async Task ProcessAsync_WhenContextIsNull_Throws()
    {
        var processor = CreateProcessor(
            new RecordingInboundMessagePipeline(),
            MessageDispatchDescriptorFactory.CreateEvent<TestEvent>());

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => processor.ProcessAsync(null!));
    }

    [Fact]
    public void Constructor_WhenDispatchCatalogIsNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => new InboundMessageProcessor(
                null!,
                new RecordingInboundMessagePipeline()));
    }

    [Fact]
    public void Constructor_WhenPipelineIsNull_Throws()
    {
        var catalog = new MessageDispatchCatalog([]);

        Assert.Throws<ArgumentNullException>(
            () => new InboundMessageProcessor(
                catalog,
                null!));
    }

    private static InboundMessageProcessor CreateProcessor(
        IInboundMessagePipeline pipeline,
        params MessageDispatchDescriptor[] descriptors)
    {
        return new InboundMessageProcessor(
            new MessageDispatchCatalog(descriptors),
            pipeline);
    }

    private sealed class RecordingInboundMessagePipeline
        : IInboundMessagePipeline
    {
        public object? EventEnvelope { get; private set; }

        public object? CommandEnvelope { get; private set; }

        public CancellationToken Token { get; private set; }

        public Task HandleEventAsync<TEvent>(
            MessageEnvelope<TEvent> envelope,
            CancellationToken token = default)
            where TEvent : class, IEvent
        {
            EventEnvelope = envelope;
            Token = token;
            return Task.CompletedTask;
        }

        public Task HandleCommandAsync<TCommand>(
            MessageEnvelope<TCommand> envelope,
            CancellationToken token = default)
            where TCommand : class, ICommand
        {
            CommandEnvelope = envelope;
            Token = token;
            return Task.CompletedTask;
        }
    }
}
