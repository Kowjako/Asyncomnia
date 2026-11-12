using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Tests.Inbound;

public sealed class MessageDispatchDescriptorFactoryTests
{
    [Fact]
    public async Task CreateEvent_DispatchesContextThroughTypedEventPipelineMethod()
    {
        var message = new TestEvent("event");
        var context = CreateContext(message);
        var pipeline = new RecordingInboundMessagePipeline();
        var descriptor = MessageDispatchDescriptorFactory.CreateEvent<TestEvent>();

        await descriptor.DispatchAsync(pipeline, context, CancellationToken.None);

        Assert.Equal(typeof(TestEvent), descriptor.MessageType);
        Assert.Equal(typeof(TestEvent), pipeline.EventMessageType);
        Assert.Same(context, pipeline.EventContext);
        Assert.Null(pipeline.CommandContext);
    }

    [Fact]
    public async Task CreateCommand_DispatchesContextThroughTypedCommandPipelineMethod()
    {
        var message = new TestCommand("command");
        var context = CreateContext(message);
        var pipeline = new RecordingInboundMessagePipeline();
        var descriptor = MessageDispatchDescriptorFactory.CreateCommand<TestCommand>();

        await descriptor.DispatchAsync(pipeline, context, CancellationToken.None);

        Assert.Equal(typeof(TestCommand), descriptor.MessageType);
        Assert.Equal(typeof(TestCommand), pipeline.CommandMessageType);
        Assert.Same(context, pipeline.CommandContext);
        Assert.Null(pipeline.EventContext);
    }

    private static InboundMessageContext CreateContext(IMessage message)
    {
        return new InboundMessageContext(
            "tests/subscription",
            message.GetType().AssemblyQualifiedName!,
            message,
            new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow));
    }

    private sealed class RecordingInboundMessagePipeline : IInboundMessagePipeline
    {
        public Type? EventMessageType { get; private set; }

        public InboundMessageContext? EventContext { get; private set; }

        public Type? CommandMessageType { get; private set; }

        public InboundMessageContext? CommandContext { get; private set; }

        public Task HandleEventAsync<TEvent>(
            InboundMessageContext context,
            CancellationToken token = default)
            where TEvent : class, IEvent
        {
            EventMessageType = typeof(TEvent);
            EventContext = context;
            return Task.CompletedTask;
        }

        public Task HandleCommandAsync<TCommand>(
            InboundMessageContext context,
            CancellationToken token = default)
            where TCommand : class, ICommand
        {
            CommandMessageType = typeof(TCommand);
            CommandContext = context;
            return Task.CompletedTask;
        }
    }
}
