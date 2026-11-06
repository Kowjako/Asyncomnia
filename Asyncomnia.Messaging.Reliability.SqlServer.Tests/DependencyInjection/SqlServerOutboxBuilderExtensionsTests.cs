using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.Migrations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.DependencyInjection;

public sealed class SqlServerOutboxBuilderExtensionsTests
{
    [Fact]
    public void UseSqlServer_CalledTwice_LeavesSingleOptionsValidatorRegistration()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
        {
            outbox.UseSqlServer(options =>
                options.ConnectionString = "first-connection-string");
            outbox.UseSqlServer(options =>
                options.ConnectionString = "second-connection-string");
        });

        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IValidateOptions<SqlServerOutboxOptions>));
    }

    [Fact]
    public void UseSqlServer_ReturnsSameOutboxBuilder()
    {
        var services = new ServiceCollection();
        OutboxBuilder? configuredBuilder = null;
        OutboxBuilder? returnedBuilder = null;

        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
        {
            configuredBuilder = outbox;
            returnedBuilder = outbox.UseSqlServer(options =>
                options.ConnectionString = "test-connection-string");
        });

        Assert.Same(configuredBuilder, returnedBuilder);
    }

    [Fact]
    public void UseSqlServer_CalledTwice_LeavesSingleScopedStoreRegistration()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
        {
            outbox.UseSqlServer(options =>
                options.ConnectionString = "first-connection-string");
            outbox.UseSqlServer(options =>
                options.ConnectionString = "second-connection-string");
        });

        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IOutboxStore));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal("SqlServerOutboxStore", registration.ImplementationType?.Name);
    }

    [Fact]
    public void UseSqlServer_CalledTwice_LeavesSingleScopedMigratorRegistration()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
        {
            outbox.UseSqlServer(options =>
                options.ConnectionString = "first-connection-string");
            outbox.UseSqlServer(options =>
                options.ConnectionString = "second-connection-string");
        });

        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(SqlServerMigrator));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal(typeof(SqlServerMigrator), registration.ImplementationType);
    }
}
