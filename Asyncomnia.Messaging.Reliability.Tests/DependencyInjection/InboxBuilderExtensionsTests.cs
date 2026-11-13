using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Inbox.Configuration;
using Asyncomnia.Messaging.Reliability.Inbox.Processing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

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

        builder.UseInbox();

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

        builder.UseInbox();
        builder.UseInbox();

        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IInboundMessagePipeline));
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(HandlerMessagePipeline));
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(InboxCleanupProcessor));
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IValidateOptions<InboxOptions>));

        var hostedService = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IHostedService));
        Assert.Equal(
            "InboxCleanupBackgroundJob",
            hostedService.ImplementationType?.Name);
    }

    [Fact]
    public void UseInbox_WithoutConfiguration_ReturnsSameMessagingBuilder()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        var returnedBuilder = builder.UseInbox();

        Assert.Same(builder, returnedBuilder);
    }
}
