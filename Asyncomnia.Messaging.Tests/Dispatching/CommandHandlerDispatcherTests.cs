using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Tests.Dispatching;

public sealed class CommandHandlerDispatcherTests
{
    [Fact]
    public async Task DispatchCommandAsync_WhenNoHandlerExists_Throws()
    {
        var services = CreateServices();
        services.AddAsyncomniaMessaging();

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var dispatcher = scope.ServiceProvider.GetRequiredService<IMessageHandlerDispatcher>();
        var envelope = TestEnvelopeFactory.Create(new TestCommand("value"));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => dispatcher.DispatchCommandAsync(envelope));

        Assert.Contains(typeof(TestCommand).FullName!, exception.Message);
    }

    [Fact]
    public async Task DispatchCommandAsync_WhenOneHandlerExists_ExecutesHandlerAndPassesMetadata()
    {
        var services = CreateServices();
        services
            .AddAsyncomniaMessaging()
            .RegisterCommandHandler<TestCommand, FirstCommandHandler>();

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var dispatcher = scope.ServiceProvider.GetRequiredService<IMessageHandlerDispatcher>();
        var probe = provider.GetRequiredService<HandlerProbe>();
        var envelope = TestEnvelopeFactory.Create(new TestCommand("value"));

        await dispatcher.DispatchCommandAsync(envelope);

        Assert.Equal(nameof(FirstCommandHandler), Assert.Single(probe.Calls));
        Assert.Same(envelope.Metadata, Assert.Single(probe.Metadata));
    }

    [Fact]
    public async Task DispatchCommandAsync_WhenMultipleHandlersExist_ThrowsWithoutExecutingHandlers()
    {
        var services = CreateServices();
        services
            .AddAsyncomniaMessaging()
            .RegisterCommandHandler<TestCommand, FirstCommandHandler>()
            .RegisterCommandHandler<TestCommand, SecondCommandHandler>();

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var dispatcher = scope.ServiceProvider.GetRequiredService<IMessageHandlerDispatcher>();
        var probe = provider.GetRequiredService<HandlerProbe>();
        var envelope = TestEnvelopeFactory.Create(new TestCommand("value"));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => dispatcher.DispatchCommandAsync(envelope));

        Assert.Contains(typeof(TestCommand).FullName!, exception.Message);
        Assert.Empty(probe.Calls);
    }

    private static ServiceCollection CreateServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton<HandlerProbe>();

        return services;
    }
}
