using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DelayedPublishing.Configuration;
using Asyncomnia.Messaging.Reliability.DelayedPublishing.Processing;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
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
            descriptor => descriptor.ServiceType ==
                typeof(IValidateOptions<DelayedPublishingOptions>));

        var hostedService = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IHostedService));
        Assert.Equal("DelayedMessageBackgroundJob", hostedService.ImplementationType?.Name);
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
