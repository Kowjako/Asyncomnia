using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Outbound;
using Asyncomnia.Messaging.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Tests.DependencyInjection;

public sealed class ServiceCollectionExtensionsTests
{
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
}
