using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.Inbox.Storage;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Migrations;
using Asyncomnia.Messaging.Reliability.SqlServer.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.DependencyInjection;

public sealed class SqlServerMessagingBuilderExtensionsTests
{
    [Fact]
    public void UseSqlServer_ReturnsSameMessagingBuilder()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        var returnedBuilder = builder.UseSqlServer(Configure);

        Assert.Same(builder, returnedBuilder);
    }

    [Fact]
    public void UseSqlServer_CalledTwice_LeavesSingleRegistrationForEachSqlServerService()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseSqlServer(Configure);
        builder.UseSqlServer(Configure);

        AssertSingleScopedRegistration<IOutboxStore>(services, "SqlServerOutboxStore");
        AssertSingleScopedRegistration<IOutboxWriter>(services, nameof(SqlServerOutboxWriter));
        AssertSingleScopedRegistration<IInboxWriter>(services, nameof(SqlServerInboxWriter));
        AssertSingleScopedRegistration<IInboxCleanupStore>(services, nameof(SqlServerInboxCleanupStore));
        AssertSingleScopedRegistration<IDelayedMessageWriter>(services, nameof(SqlServerDelayedMessageWriter));
        AssertSingleScopedRegistration<IDelayedMessageStore>(services, nameof(SqlServerDelayedMessageStore));
        AssertSingleScopedRegistration<SqlServerMigrator>(services, nameof(SqlServerMigrator));
        AssertSingleScopedRegistration<IDbTransactionAccessor>(services, "StandaloneDbTransactionAccessor");
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IValidateOptions<SqlServerOptions>));
    }

    [Fact]
    public void UseSqlServer_WithConfiguration_UsesConfiguredConnectionString()
    {
        var services = new ServiceCollection();
        services.AddAsyncomniaMessaging().UseSqlServer(Configure);
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<SqlServerOptions>>().Value;

        Assert.Equal("test-connection-string", options.ConnectionString);
    }

    private static void Configure(SqlServerOptions options)
    {
        options.ConnectionString = "test-connection-string";
    }

    private static void AssertSingleScopedRegistration<TService>(
        IServiceCollection services,
        string implementationTypeName)
    {
        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(TService));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal(implementationTypeName, registration.ImplementationType?.Name);
    }
}
