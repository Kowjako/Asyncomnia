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
        await using var scope = provider.CreateAsyncScope();
        var dispatcher = scope.ServiceProvider.GetRequiredService<IMessageHandlerDispatcher>();
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
        await using var scope = provider.CreateAsyncScope();
        var dispatcher = scope.ServiceProvider.GetRequiredService<IMessageHandlerDispatcher>();
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
        await using var scope = provider.CreateAsyncScope();
        var dispatcher = scope.ServiceProvider.GetRequiredService<IMessageHandlerDispatcher>();
        var probe = provider.GetRequiredService<HandlerProbe>();
        var envelope = TestEnvelopeFactory.Create(new TestEvent("value"));

        await dispatcher.DispatchEventAsync(envelope);

        Assert.Equal(2, probe.Calls.Count);
        Assert.Contains(nameof(FirstEventHandler), probe.Calls);
        Assert.Contains(nameof(SecondEventHandler), probe.Calls);
    }

    [Fact]
    public async Task DispatchEventAsync_UsesHandlersFromCurrentScope()
    {
        var services = CreateServices();
        services
            .AddAsyncomniaMessaging()
            .RegisterEventHandler<TestEvent, FirstEventHandler>();

        await using var provider = services.BuildServiceProvider();
        var probe = provider.GetRequiredService<HandlerProbe>();

        await using var firstScope = provider.CreateAsyncScope();
        var firstDispatcher = firstScope.ServiceProvider.GetRequiredService<IMessageHandlerDispatcher>();

        await firstDispatcher.DispatchEventAsync(TestEnvelopeFactory.Create(new TestEvent("first")));
        await firstDispatcher.DispatchEventAsync(TestEnvelopeFactory.Create(new TestEvent("second")));

        await using var secondScope = provider.CreateAsyncScope();
        var secondDispatcher = secondScope.ServiceProvider.GetRequiredService<IMessageHandlerDispatcher>();

        await secondDispatcher.DispatchEventAsync(TestEnvelopeFactory.Create(new TestEvent("third")));

        Assert.Equal(3, probe.HandlerInstances.Count);
        Assert.Equal(probe.HandlerInstances[0], probe.HandlerInstances[1]);
        Assert.NotEqual(probe.HandlerInstances[0], probe.HandlerInstances[2]);
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
        await using var scope = provider.CreateAsyncScope();
        var dispatcher = scope.ServiceProvider.GetRequiredService<IMessageHandlerDispatcher>();
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
