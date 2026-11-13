using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Outbound;
using Asyncomnia.Messaging.Reliability.DelayedPublishing.Configuration;
using Asyncomnia.Messaging.Reliability.DelayedPublishing.Processing;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Asyncomnia.Messaging.Reliability.Outbox.Processing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Tests.DependencyInjection;

public sealed class DelayedPublishingBuilderExtensionsTests
{
    [Fact]
    public void UseDelayedPublishing_ReturnsSameMessagingBuilderAndInvokesConfiguration()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();
        DelayedPublishingBuilder? configuredBuilder = null;

        var returnedBuilder = builder.UseDelayedPublishing(delayed =>
            configuredBuilder = delayed);

        Assert.Same(builder, returnedBuilder);
        Assert.NotNull(configuredBuilder);
        Assert.Same(services, configuredBuilder.Services);
    }

    [Fact]
    public void UseDelayedPublishing_CalledTwice_LeavesSingleRegistrationForEachService()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseDelayedPublishing();
        builder.UseDelayedPublishing();

        var scheduler = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IMessageScheduler));
        Assert.Equal(ServiceLifetime.Scoped, scheduler.Lifetime);
        Assert.Equal("MessageScheduler", scheduler.ImplementationType?.Name);

        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(DelayedMessageBatchProcessor));
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(OutboxBatchProcessor));
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType ==
                typeof(IValidateOptions<DelayedPublishingOptions>));
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType ==
                typeof(IValidateOptions<OutboxOptions>));

        var hostedServices = services
            .Where(descriptor => descriptor.ServiceType == typeof(IHostedService))
            .ToArray();
        Assert.Equal(2, hostedServices.Length);
        Assert.Contains(
            hostedServices,
            descriptor => descriptor.ImplementationType?.Name == "DelayedMessageBackgroundJob");
        Assert.Contains(
            hostedServices,
            descriptor => descriptor.ImplementationType?.Name == "OutboxBackgroundJob");
    }

    [Fact]
    public void UseDelayedPublishing_RegistersOutboxProcessingWithoutReplacingOutboundPipeline()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseDelayedPublishing();

        var pipeline = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IOutboundMessagePipeline));
        Assert.Equal("TransportMessagePipeline", pipeline.ImplementationType?.Name);
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(OutboxBatchProcessor));
        Assert.Contains(
            services,
            descriptor => descriptor.ServiceType == typeof(IHostedService) &&
                descriptor.ImplementationType?.Name == "OutboxBackgroundJob");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void UseDelayedPublishing_WithUseOutbox_RegistersSharedOutboxProcessingOnce(
        bool configureOutboxFirst)
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        if (configureOutboxFirst)
        {
            builder.UseOutbox();
            builder.UseDelayedPublishing();
        }
        else
        {
            builder.UseDelayedPublishing();
            builder.UseOutbox();
        }

        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(OutboxBatchProcessor));
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IValidateOptions<OutboxOptions>));
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IHostedService) &&
                descriptor.ImplementationType?.Name == "OutboxBackgroundJob");
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IHostedService) &&
                descriptor.ImplementationType?.Name == "DelayedMessageBackgroundJob");
    }

    [Fact]
    public void UseDelayedPublishing_WithoutConfiguration_ReturnsSameMessagingBuilder()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        var returnedBuilder = builder.UseDelayedPublishing();

        Assert.Same(builder, returnedBuilder);
    }
}
