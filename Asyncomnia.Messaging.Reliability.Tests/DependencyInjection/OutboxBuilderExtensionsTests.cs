using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Outbound;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Asyncomnia.Messaging.Reliability.Outbox.Processing;
using Microsoft.Extensions.DependencyInjection;
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
    }

    [Fact]
    public void UseOutbox_CalledTwice_LeavesSingleOptionsValidatorRegistration()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseOutbox();
        builder.UseOutbox();

        Assert.Single(services, x => x.ServiceType == typeof(IValidateOptions<OutboxOptions>));
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
}
