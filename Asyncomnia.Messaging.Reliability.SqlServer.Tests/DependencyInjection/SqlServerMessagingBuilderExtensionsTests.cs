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
using Asyncomnia.Messaging.Reliability.SqlServer.Infrastructure.Connection;
using Asyncomnia.Messaging.Reliability.SqlServer.Migrations;
using Asyncomnia.Messaging.Reliability.SqlServer.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Outbox.Retention;
using Asyncomnia.Messaging.Reliability.SqlServer.Sagas;
using Asyncomnia.Messaging.Reliability.SqlServer.Sagas.Retention;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Asyncomnia.Messaging.Sagas.Persistence;
using Asyncomnia.Messaging.Sagas.Persistence.Retention;
using Asyncomnia.Messaging.Sagas.StateMachine;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.DependencyInjection;

public sealed class SqlServerMessagingBuilderExtensionsTests
{
    [Fact]
    public void UseSqlServer_WhenBuilderIsNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            SqlServerMessagingBuilderExtensions.UseSqlServer(null!, _ => { }));
    }

    [Fact]
    public void UseSqlServer_WhenConfigureIsNull_ThrowsArgumentNullException()
    {
        var builder = new ServiceCollection().AddAsyncomniaMessaging();

        Assert.Throws<ArgumentNullException>(() =>
            builder.UseSqlServer(null!));
    }

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
        AssertSingleScopedRegistration<IOutboxCleanupStore>(services, nameof(SqlServerOutboxCleanupStore));
        AssertSingleScopedRegistration<IOutboxWriter>(services, nameof(SqlServerOutboxWriter));
        AssertSingleScopedRegistration<IInboxWriter>(services, nameof(SqlServerInboxWriter));
        AssertSingleScopedRegistration<IInboxCleanupStore>(services, nameof(SqlServerInboxCleanupStore));
        AssertSingleScopedRegistration<IDelayedMessageWriter>(services, nameof(SqlServerDelayedMessageWriter));
        AssertSingleScopedRegistration<IDelayedMessageStore>(services, nameof(SqlServerDelayedMessageStore));
        AssertSingleScopedRegistration<SqlServerMigrator>(services, nameof(SqlServerMigrator));
        AssertSingleScopedRegistration<IDbTransactionAccessor>(services, "StandaloneDbTransactionAccessor");
        AssertSingleScopedOpenGenericRegistration(
            services,
            typeof(ISagaRepository<>),
            typeof(SqlServerSagaRepository<>));
        AssertSingleScopedRegistration<ISagaCleanupStore>(
            services,
            nameof(SqlServerSagaCleanupStore));
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(ISqlServerConnectionFactory));
    }

    [Fact]
    public void UseSqlServer_RegistersClosedSagaRepositoryFromOpenGenericRegistration()
    {
        var services = new ServiceCollection();
        services.AddAsyncomniaMessaging().UseSqlServer(Configure);
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var repository = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<DependencyInjectionTestSaga>>();

        Assert.IsType<SqlServerSagaRepository<DependencyInjectionTestSaga>>(repository);
    }

    [Fact]
    public void UseSqlServer_WithoutConnectionSource_ThrowsInvalidOperationException()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            builder.UseSqlServer(_ => { }));

        Assert.Contains("SQL Server connection source is not configured", exception.Message);
    }

    private static void Configure(SqlServerBuilder builder)
    {
        builder.UseConnectionString("test-connection-string");
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

    private static void AssertSingleScopedOpenGenericRegistration(
        IServiceCollection services,
        Type serviceType,
        Type implementationType)
    {
        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == serviceType);

        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal(implementationType, registration.ImplementationType);
    }

    private sealed class DependencyInjectionTestSaga : AsyncomniaSagaInstance;
}
