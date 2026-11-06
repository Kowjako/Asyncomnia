using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.DependencyInjection;

public sealed class EntityFrameworkCoreOutboxBuilderExtensionsTests
{
    [Fact]
    public void UseEntityFrameworkCore_ReturnsSameOutboxBuilder()
    {
        var services = new ServiceCollection();
        OutboxBuilder? configuredBuilder = null;
        OutboxBuilder? returnedBuilder = null;

        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
        {
            configuredBuilder = outbox;
            returnedBuilder = outbox.UseEntityFrameworkCore<TestDbContext>();
        });

        Assert.Same(configuredBuilder, returnedBuilder);
    }

    [Fact]
    public void UseEntityFrameworkCore_CalledTwice_LeavesSingleScopedWriterRegistration()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
        {
            outbox.UseEntityFrameworkCore<TestDbContext>();
            outbox.UseEntityFrameworkCore<TestDbContext>();
        });

        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IOutboxWriter));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal(
            typeof(EntityFrameworkCoreOutboxWriter<TestDbContext>),
            registration.ImplementationType);
    }
}
