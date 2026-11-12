using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.Migrations;
using Asyncomnia.Messaging.Reliability.SqlServer.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Tests.TestDoubles;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.DependencyInjection;

public sealed class SqlServerOutboxBuilderExtensionsTests
{
    [Fact]
    public void UseSqlServer_ReturnsSameOutboxBuilder()
    {
        var services = new ServiceCollection();
        OutboxBuilder? configuredBuilder = null;
        OutboxBuilder? returnedBuilder = null;

        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
        {
            configuredBuilder = outbox;
            returnedBuilder = outbox.UseSqlServer(Configure);
        });

        Assert.Same(configuredBuilder, returnedBuilder);
    }

    [Fact]
    public void UseSqlServer_WithTransactionAccessor_ReturnsSameOutboxBuilder()
    {
        var services = new ServiceCollection();
        OutboxBuilder? configuredBuilder = null;
        OutboxBuilder? returnedBuilder = null;

        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
        {
            configuredBuilder = outbox;
            returnedBuilder = outbox.UseSqlServer<ITestUnitOfWork>(Configure);
        });

        Assert.Same(configuredBuilder, returnedBuilder);
    }

    [Fact]
    public void UseSqlServer_CalledTwice_LeavesSingleRegistrationForEachSqlServerService()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
        {
            outbox.UseSqlServer(Configure);
            outbox.UseSqlServer(Configure);
        });

        AssertSingleScopedRegistration<IOutboxStore>(services, "SqlServerOutboxStore");
        AssertSingleScopedRegistration<SqlServerMigrator>(services, nameof(SqlServerMigrator));
        AssertSingleScopedRegistration<IOutboxWriter>(services, nameof(SqlServerOutboxWriter));
        AssertSingleScopedRegistration<IDbTransactionAccessor>(services, "StandaloneDbTransactionAccessor");
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IValidateOptions<SqlServerOptions>));
    }

    [Fact]
    public void UseSqlServer_WithTransactionAccessorCalledTwice_LeavesSingleAccessorAndWriterRegistration()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
        {
            outbox.UseSqlServer<ITestUnitOfWork>(Configure);
            outbox.UseSqlServer<ITestUnitOfWork>(Configure);
        });

        var accessorRegistration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IDbTransactionAccessor));
        Assert.Equal(ServiceLifetime.Scoped, accessorRegistration.Lifetime);
        Assert.NotNull(accessorRegistration.ImplementationFactory);

        AssertSingleScopedRegistration<IOutboxWriter>(services, nameof(SqlServerOutboxWriter));
    }

    [Fact]
    public void UseSqlServer_WithInterfaceAccessor_ResolvesSameScopedInstance()
    {
        var services = new ServiceCollection();
        services.AddScoped<ITestUnitOfWork, TestUnitOfWork>();
        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
            outbox.UseSqlServer<ITestUnitOfWork>(Configure));
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var unitOfWork = scope.ServiceProvider.GetRequiredService<ITestUnitOfWork>();
        var transactionAccessor = scope.ServiceProvider.GetRequiredService<IDbTransactionAccessor>();

        Assert.Same(unitOfWork, transactionAccessor);
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
