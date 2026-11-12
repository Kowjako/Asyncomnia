using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Reliability.Tests.DependencyInjection;

public sealed class InboxBuilderExtensionsTests
{
    [Fact]
    public void UseInbox_ReturnsSameMessagingBuilderAndInvokesConfiguration()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();
        InboxBuilder? configuredBuilder = null;

        var returnedBuilder = builder.UseInbox(inbox =>
            configuredBuilder = inbox);

        Assert.Same(builder, returnedBuilder);
        Assert.NotNull(configuredBuilder);
        Assert.Same(services, configuredBuilder.Services);
    }

    [Fact]
    public void UseInbox_ReplacesDefaultInboundPipelineAndRegistersHandlerPipeline()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseInbox(_ => { });

        var pipelineRegistration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IInboundMessagePipeline));
        Assert.Equal(ServiceLifetime.Scoped, pipelineRegistration.Lifetime);
        Assert.Equal("InboxMessagePipeline", pipelineRegistration.ImplementationType?.Name);

        var handlerPipelineRegistration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(HandlerMessagePipeline));
        Assert.Equal(ServiceLifetime.Scoped, handlerPipelineRegistration.Lifetime);
    }

    [Fact]
    public void UseInbox_CalledTwice_LeavesSinglePipelineRegistration()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseInbox(_ => { });
        builder.UseInbox(_ => { });

        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IInboundMessagePipeline));
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(HandlerMessagePipeline));
    }

    [Fact]
    public void UseInbox_WhenConfigureIsNull_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        Assert.Throws<ArgumentNullException>(() =>
            builder.UseInbox(null!));
    }
}
