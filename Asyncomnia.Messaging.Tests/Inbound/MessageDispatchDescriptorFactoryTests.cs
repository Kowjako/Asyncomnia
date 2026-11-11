using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Tests.Inbound;

public sealed class MessageDispatchDescriptorFactoryTests
{
    [Fact]
    public async Task CreateEvent_DispatchesTypedEnvelopeToEventPipeline()
    {
        var message = new TestEvent("event");
        var metadata = new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow);
        var pipeline = new RecordingInboundMessagePipeline();
        var descriptor = MessageDispatchDescriptorFactory.CreateEvent<TestEvent>();

        await descriptor.DispatchAsync(
            pipeline,
            message,
            metadata,
            CancellationToken.None);

        Assert.Equal(typeof(TestEvent), descriptor.MessageType);
        var envelope = Assert.IsType<MessageEnvelope<TestEvent>>(pipeline.EventEnvelope);
        Assert.Same(message, envelope.Message);
        Assert.Same(metadata, envelope.Metadata);
        Assert.Null(pipeline.CommandEnvelope);
    }

    [Fact]
    public async Task CreateCommand_DispatchesTypedEnvelopeToCommandPipeline()
    {
        var message = new TestCommand("command");
        var metadata = new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow);
        var pipeline = new RecordingInboundMessagePipeline();
        var descriptor = MessageDispatchDescriptorFactory.CreateCommand<TestCommand>();

        await descriptor.DispatchAsync(
            pipeline,
            message,
            metadata,
            CancellationToken.None);

        Assert.Equal(typeof(TestCommand), descriptor.MessageType);
        var envelope = Assert.IsType<MessageEnvelope<TestCommand>>(pipeline.CommandEnvelope);
        Assert.Same(message, envelope.Message);
        Assert.Same(metadata, envelope.Metadata);
        Assert.Null(pipeline.EventEnvelope);
    }

    private sealed class RecordingInboundMessagePipeline : IInboundMessagePipeline
    {
        public object? EventEnvelope { get; private set; }

        public object? CommandEnvelope { get; private set; }

        public Task HandleEventAsync<TEvent>(
            MessageEnvelope<TEvent> envelope,
            CancellationToken token = default)
            where TEvent : class, IEvent
        {
            EventEnvelope = envelope;
            return Task.CompletedTask;
        }

        public Task HandleCommandAsync<TCommand>(
            MessageEnvelope<TCommand> envelope,
            CancellationToken token = default)
            where TCommand : class, ICommand
        {
            CommandEnvelope = envelope;
            return Task.CompletedTask;
        }
    }
}
