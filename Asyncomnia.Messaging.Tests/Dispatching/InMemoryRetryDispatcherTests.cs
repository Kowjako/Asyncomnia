using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Tests.Dispatching;

public sealed class InMemoryRetryDispatcherTests
{
    [Fact]
    public async Task DispatchEventAsync_RetriesOnlyFailingHandler()
    {
        var probe = new InMemoryRetryHandlerProbe();
        var services = new ServiceCollection();
        services.AddSingleton(probe);
        services
            .AddAsyncomniaMessaging()
            .RegisterEventHandler<
                TestEvent,
                SuccessfulRetryEventHandler>()
            .RegisterEventHandler<
                TestEvent,
                TransientRetryEventHandler>()
            .UseInMemoryRetries(options => options.Immediately(2));
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var dispatcher = scope.ServiceProvider
            .GetRequiredService<IMessageHandlerDispatcher>();

        await dispatcher.DispatchEventAsync(
            TestEnvelopeFactory.Create(new TestEvent("value")));

        Assert.Equal(1, probe.SuccessfulEventCalls);
        Assert.Equal(3, probe.TransientEventCalls);
    }

    [Fact]
    public async Task DispatchCommandAsync_WhenRetriesAreExhausted_PropagatesLastFailure()
    {
        var probe = new InMemoryRetryHandlerProbe();
        var services = new ServiceCollection();
        services.AddSingleton(probe);
        services
            .AddAsyncomniaMessaging()
            .RegisterCommandHandler<
                TestCommand,
                FailingRetryCommandHandler>()
            .UseInMemoryRetries(options => options.Immediately(2));
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var dispatcher = scope.ServiceProvider
            .GetRequiredService<IMessageHandlerDispatcher>();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => dispatcher.DispatchCommandAsync(
                TestEnvelopeFactory.Create(new TestCommand("value"))));

        Assert.Same(probe.CommandException, exception);
        Assert.Equal(3, probe.CommandCalls);
    }

    [Fact]
    public async Task DispatchCommandAsync_WithoutRetryConfiguration_ExecutesHandlerOnce()
    {
        var probe = new InMemoryRetryHandlerProbe();
        var services = new ServiceCollection();
        services.AddSingleton(probe);
        services
            .AddAsyncomniaMessaging()
            .RegisterCommandHandler<
                TestCommand,
                FailingRetryCommandHandler>();
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var dispatcher = scope.ServiceProvider
            .GetRequiredService<IMessageHandlerDispatcher>();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => dispatcher.DispatchCommandAsync(
                TestEnvelopeFactory.Create(new TestCommand("value"))));

        Assert.Equal(1, probe.CommandCalls);
    }
}
