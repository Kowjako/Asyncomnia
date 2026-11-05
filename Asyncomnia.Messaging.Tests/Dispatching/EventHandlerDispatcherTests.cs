using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Tests.Dispatching;

public sealed class EventHandlerDispatcherTests
{
    [Fact]
    public async Task DispatchEventAsync_WhenNoHandlerExists_Throws()
    {
        var services = CreateServices();
        services.AddAsyncomniaMessaging();

        await using var provider = services.BuildServiceProvider();
        var dispatcher = provider.GetRequiredService<IMessageHandlerDispatcher>();
        var envelope = TestEnvelopeFactory.Create(new TestEvent("value"));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => dispatcher.DispatchEventAsync(envelope));

        Assert.Contains(typeof(TestEvent).FullName!, exception.Message);
    }

    [Fact]
    public async Task DispatchEventAsync_WhenOneHandlerExists_ExecutesHandlerAndPassesMetadata()
    {
        var services = CreateServices();
        services
            .AddAsyncomniaMessaging()
            .RegisterEventHandler<TestEvent, FirstEventHandler>();

        await using var provider = services.BuildServiceProvider();
        var dispatcher = provider.GetRequiredService<IMessageHandlerDispatcher>();
        var probe = provider.GetRequiredService<HandlerProbe>();
        var envelope = TestEnvelopeFactory.Create(new TestEvent("value"));

        await dispatcher.DispatchEventAsync(envelope);

        Assert.Equal(nameof(FirstEventHandler), Assert.Single(probe.Calls));
        Assert.Same(envelope.Metadata, Assert.Single(probe.Metadata));
    }

    [Fact]
    public async Task DispatchEventAsync_WhenMultipleHandlersExist_ExecutesAllHandlers()
    {
        var services = CreateServices();
        services
            .AddAsyncomniaMessaging()
            .RegisterEventHandler<TestEvent, FirstEventHandler>()
            .RegisterEventHandler<TestEvent, SecondEventHandler>();

        await using var provider = services.BuildServiceProvider();
        var dispatcher = provider.GetRequiredService<IMessageHandlerDispatcher>();
        var probe = provider.GetRequiredService<HandlerProbe>();
        var envelope = TestEnvelopeFactory.Create(new TestEvent("value"));

        await dispatcher.DispatchEventAsync(envelope);

        Assert.Equal(2, probe.Calls.Count);
        Assert.Contains(nameof(FirstEventHandler), probe.Calls);
        Assert.Contains(nameof(SecondEventHandler), probe.Calls);
    }

    [Fact]
    public async Task DispatchEventAsync_CreatesNewScopeForEveryDispatch()
    {
        var services = CreateServices();
        services
            .AddAsyncomniaMessaging()
            .RegisterEventHandler<TestEvent, FirstEventHandler>();

        await using var provider = services.BuildServiceProvider();
        var dispatcher = provider.GetRequiredService<IMessageHandlerDispatcher>();
        var probe = provider.GetRequiredService<HandlerProbe>();

        await dispatcher.DispatchEventAsync(TestEnvelopeFactory.Create(new TestEvent("first")));
        await dispatcher.DispatchEventAsync(TestEnvelopeFactory.Create(new TestEvent("second")));

        Assert.Equal(2, probe.HandlerInstances.Count);
        Assert.Equal(2, probe.HandlerInstances.Distinct().Count());
    }

    [Fact]
    public async Task RegisterEventHandler_WhenCalledTwice_DoesNotDuplicateHandler()
    {
        var services = CreateServices();
        services
            .AddAsyncomniaMessaging()
            .RegisterEventHandler<TestEvent, FirstEventHandler>()
            .RegisterEventHandler<TestEvent, FirstEventHandler>();

        await using var provider = services.BuildServiceProvider();
        var dispatcher = provider.GetRequiredService<IMessageHandlerDispatcher>();
        var probe = provider.GetRequiredService<HandlerProbe>();

        await dispatcher.DispatchEventAsync(TestEnvelopeFactory.Create(new TestEvent("value")));

        Assert.Equal(nameof(FirstEventHandler), Assert.Single(probe.Calls));
    }

    private static ServiceCollection CreateServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton<HandlerProbe>();

        return services;
    }
}
