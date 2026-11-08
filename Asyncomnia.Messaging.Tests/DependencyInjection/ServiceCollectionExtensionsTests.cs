using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Outbound;
using Asyncomnia.Messaging.Retry;
using Asyncomnia.Messaging.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Tests.DependencyInjection;

public sealed class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddAsyncomniaMessaging_RegistersMessageHandlerDispatcherAsScoped()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging();

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IMessageHandlerDispatcher));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal("MessageHandlerDispatcher", registration.ImplementationType?.Name);
    }

    [Fact]
    public void AddAsyncomniaMessaging_RegistersInboundPipelineAsScoped()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging();

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IInboundMessagePipeline));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal("HandlerMessagePipeline", registration.ImplementationType?.Name);
    }

    [Fact]
    public void AddAsyncomniaMessaging_WhenInboundPipelineIsAlreadyRegistered_DoesNotReplaceIt()
    {
        var services = new ServiceCollection();
        var pipeline = new CapturingInboundMessagePipeline();
        services.AddSingleton<IInboundMessagePipeline>(pipeline);

        services.AddAsyncomniaMessaging();

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IInboundMessagePipeline));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Same(pipeline, registration.ImplementationInstance);
    }

    [Fact]
    public void AddAsyncomniaMessaging_RegistersTransportPipelineAsScoped()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging();

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IOutboundMessagePipeline));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal("TransportMessagePipeline", registration.ImplementationType?.Name);
    }

    [Fact]
    public void AddAsyncomniaMessaging_WhenPipelineIsAlreadyRegistered_DoesNotReplaceIt()
    {
        var services = new ServiceCollection();
        var pipeline = new CapturingOutboundMessagePipeline();
        services.AddSingleton<IOutboundMessagePipeline>(pipeline);

        services.AddAsyncomniaMessaging();

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IOutboundMessagePipeline));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Same(pipeline, registration.ImplementationInstance);
    }

    [Fact]
    public void AddAsyncomniaMessaging_RegistersInMemoryRetryExecutorAsSingleton()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging();

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IInMemoryRetryExecutor));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(
            typeof(InMemoryRetryExecutor),
            registration.ImplementationType);
    }

    [Fact]
    public void AddAsyncomniaMessaging_CanResolveDispatcherWithoutLoggingRegistration()
    {
        var services = new ServiceCollection();
        services.AddAsyncomniaMessaging();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var dispatcher = scope.ServiceProvider
            .GetRequiredService<IMessageHandlerDispatcher>();

        Assert.NotNull(dispatcher);
    }
}
