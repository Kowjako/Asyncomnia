using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Outbound;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Asyncomnia.Messaging.Reliability.Outbox.Processing;
using Asyncomnia.Messaging.Reliability.Outbox.Processing.Retention;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Moq;

namespace Asyncomnia.Messaging.Reliability.Tests.DependencyInjection;

public sealed class OutboxBuilderExtensionsTests
{
    [Fact]
    public void UseOutbox_ReplacesExistingPipelineWithScopedOutboxPipeline()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new Mock<IOutboundMessagePipeline>().Object);
        var builder = services.AddAsyncomniaMessaging();

        builder.UseOutbox();

        var registration = Assert.Single(services, x => x.ServiceType == typeof(IOutboundMessagePipeline));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal("OutboxMessagePipeline", registration.ImplementationType?.Name);
    }

    [Fact]
    public void UseOutbox_CalledTwice_LeavesSinglePipelineRegistration()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseOutbox();
        builder.UseOutbox();

        Assert.Single(services, x => x.ServiceType == typeof(IOutboundMessagePipeline));
        var featureMarker = Assert.Single(
            services,
            x => x.ServiceType == typeof(OutboxFeatureRegistration));
        Assert.Equal(ServiceLifetime.Singleton, featureMarker.Lifetime);
    }

    [Fact]
    public void UseOutbox_CalledTwice_LeavesSingleOptionsValidatorRegistration()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseOutbox();
        builder.UseOutbox();

        var registrations = services
            .Where(x => x.ServiceType == typeof(IValidateOptions<OutboxOptions>))
            .ToArray();

        Assert.Equal(2, registrations.Length);
        Assert.Single(
            registrations,
            x => x.ImplementationType == typeof(OutboxOptionsValidator));
        Assert.Single(
            registrations,
            x => x.ImplementationType == typeof(OutboxStorageOptionsValidator));
    }

    [Fact]
    public void UseOutbox_CalledTwice_LeavesSingleScopedBatchProcessorRegistration()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseOutbox();
        builder.UseOutbox();

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(OutboxBatchProcessor));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
    }

    [Fact]
    public void UseOutbox_CalledTwice_LeavesSingleBackgroundJobRegistration()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseOutbox();
        builder.UseOutbox();

        var registrations = services
            .Where(x => x.ServiceType == typeof(IHostedService))
            .ToArray();

        Assert.Equal(2, registrations.Length);
        Assert.All(
            registrations,
            registration => Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime));
        Assert.Contains(
            registrations,
            registration => registration.ImplementationType?.Name == "OutboxBackgroundJob");
        Assert.Contains(
            registrations,
            registration => registration.ImplementationType?.Name == "OutboxCleanupBackgroundJob");
    }

    [Fact]
    public void UseOutbox_CalledTwice_LeavesSingleScopedCleanupProcessorRegistration()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseOutbox();
        builder.UseOutbox();

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(OutboxCleanupProcessor));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
    }
}
