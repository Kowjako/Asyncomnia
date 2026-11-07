using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Tests.Inbound;

public sealed class HandlerMessagePipelineTests
{
    [Fact]
    public async Task HandleEventAsync_DelegatesEnvelopeAndCancellationTokenToDispatcher()
    {
        var dispatcher = new CapturingMessageHandlerDispatcher();
        var services = CreateServices(dispatcher);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<IInboundMessagePipeline>();
        var envelope = TestEnvelopeFactory.Create(new TestEvent("value"));
        using var cts = new CancellationTokenSource();

        await pipeline.HandleEventAsync(envelope, cts.Token);

        Assert.Same(envelope, dispatcher.EventEnvelope);
        Assert.Equal(cts.Token, dispatcher.EventCancellationToken);
    }

    [Fact]
    public async Task HandleCommandAsync_DelegatesEnvelopeAndCancellationTokenToDispatcher()
    {
        var dispatcher = new CapturingMessageHandlerDispatcher();
        var services = CreateServices(dispatcher);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<IInboundMessagePipeline>();
        var envelope = TestEnvelopeFactory.Create(new TestCommand("value"));
        using var cts = new CancellationTokenSource();

        await pipeline.HandleCommandAsync(envelope, cts.Token);

        Assert.Same(envelope, dispatcher.CommandEnvelope);
        Assert.Equal(cts.Token, dispatcher.CommandCancellationToken);
    }

    [Fact]
    public async Task HandleEventAsync_WhenEnvelopeIsNull_Throws()
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
    public async Task HandleCommandAsync_WhenEnvelopeIsNull_Throws()
    {
        var dispatcher = new CapturingMessageHandlerDispatcher();
        var services = CreateServices(dispatcher);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<IInboundMessagePipeline>();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => pipeline.HandleCommandAsync<TestCommand>(null!));
    }

    private static ServiceCollection CreateServices(IMessageHandlerDispatcher dispatcher)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMessageHandlerDispatcher>(dispatcher);
        services.AddAsyncomniaMessaging();

        return services;
    }
}
