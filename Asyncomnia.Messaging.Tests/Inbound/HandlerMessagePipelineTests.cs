using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Tests.Inbound;

public sealed class HandlerMessagePipelineTests
{
    [Fact]
    public async Task HandleEventAsync_CreatesTypedEnvelopeAndDelegatesItToDispatcher()
    {
        var dispatcher = new CapturingMessageHandlerDispatcher();
        var services = CreateServices(dispatcher);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<IInboundMessagePipeline>();
        var message = new TestEvent("value");
        var metadata = new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow);
        var context = CreateContext(message, metadata);
        using var cancellation = new CancellationTokenSource();

        await pipeline.HandleEventAsync<TestEvent>(context, cancellation.Token);

        var envelope = Assert.IsType<MessageEnvelope<TestEvent>>(dispatcher.EventEnvelope);
        Assert.Same(message, envelope.Message);
        Assert.Same(metadata, envelope.Metadata);
        Assert.Equal(cancellation.Token, dispatcher.EventCancellationToken);
    }

    [Fact]
    public async Task HandleCommandAsync_CreatesTypedEnvelopeAndDelegatesItToDispatcher()
    {
        var dispatcher = new CapturingMessageHandlerDispatcher();
        var services = CreateServices(dispatcher);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<IInboundMessagePipeline>();
        var message = new TestCommand("value");
        var metadata = new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow);
        var context = CreateContext(message, metadata);
        using var cancellation = new CancellationTokenSource();

        await pipeline.HandleCommandAsync<TestCommand>(context, cancellation.Token);

        var envelope = Assert.IsType<MessageEnvelope<TestCommand>>(dispatcher.CommandEnvelope);
        Assert.Same(message, envelope.Message);
        Assert.Same(metadata, envelope.Metadata);
        Assert.Equal(cancellation.Token, dispatcher.CommandCancellationToken);
    }

    [Fact]
    public async Task HandleEventAsync_WhenContextIsNull_ThrowsArgumentNullException()
    {
        var dispatcher = new CapturingMessageHandlerDispatcher();
        var services = CreateServices(dispatcher);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<IInboundMessagePipeline>();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => pipeline.HandleEventAsync<TestEvent>(null!));
    }

    [Fact]
    public async Task HandleCommandAsync_WhenContextIsNull_ThrowsArgumentNullException()
    {
        var dispatcher = new CapturingMessageHandlerDispatcher();
        var services = CreateServices(dispatcher);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<IInboundMessagePipeline>();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => pipeline.HandleCommandAsync<TestCommand>(null!));
    }

    private static InboundMessageContext CreateContext(IMessage message, MessageMetadata metadata)
    {
        return new InboundMessageContext(
            "tests/subscription",
            message.GetType().AssemblyQualifiedName!,
            message,
            metadata);
    }

    private static ServiceCollection CreateServices(IMessageHandlerDispatcher dispatcher)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMessageHandlerDispatcher>(dispatcher);
        services.AddAsyncomniaMessaging();

        return services;
    }
}
