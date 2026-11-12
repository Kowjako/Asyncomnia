using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Migrations;
using Asyncomnia.Messaging.Reliability.SqlServer.Tests.TestDoubles;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.DependencyInjection;

public sealed class SqlServerInboxBuilderExtensionsTests
{
    [Fact]
    public void UseSqlServer_ReturnsSameInboxBuilder()
    {
        var services = new ServiceCollection();
        InboxBuilder? configuredBuilder = null;
        InboxBuilder? returnedBuilder = null;

        services.AddAsyncomniaMessaging().UseInbox(inbox =>
        {
            configuredBuilder = inbox;
            returnedBuilder = inbox.UseSqlServer<ITestUnitOfWork>(Configure);
        });

        Assert.Same(configuredBuilder, returnedBuilder);
    }

    [Fact]
    public void UseSqlServer_WithConfiguration_UsesConfiguredConnectionString()
    {
        var services = new ServiceCollection();
        services.AddAsyncomniaMessaging().UseInbox(inbox =>
            inbox.UseSqlServer<ITestUnitOfWork>(Configure));
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<SqlServerOptions>>().Value;

        Assert.Equal("test-connection-string", options.ConnectionString);
    }

    [Fact]
    public void UseSqlServer_WithoutConfiguration_UsesOptionsConfiguredByOutbox()
    {
        var services = new ServiceCollection();
        services.AddScoped<ITestUnitOfWork, TestUnitOfWork>();
        services
            .AddAsyncomniaMessaging()
            .UseOutbox(outbox =>
                outbox.UseSqlServer<ITestUnitOfWork>(Configure))
            .UseInbox(inbox =>
                inbox.UseSqlServer<ITestUnitOfWork>());
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<SqlServerOptions>>().Value;

        Assert.Equal("test-connection-string", options.ConnectionString);
    }

    [Fact]
    public void UseSqlServer_CalledTwice_LeavesSingleRegistrationForEachSqlServerService()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging().UseInbox(inbox =>
        {
            inbox.UseSqlServer<ITestUnitOfWork>(Configure);
            inbox.UseSqlServer<ITestUnitOfWork>();
        });

        AssertSingleScopedRegistration<IInboxWriter>(services, nameof(SqlServerInboxWriter));
        AssertSingleScopedRegistration<SqlServerMigrator>(services, nameof(SqlServerMigrator));
        AssertSingleFactoryRegistration<IInboundTransaction>(services);
        AssertSingleFactoryRegistration<IInboxTransaction>(services);
        AssertSingleFactoryRegistration<IDbTransactionAccessor>(services);
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IValidateOptions<SqlServerOptions>));
    }

    [Fact]
    public void UseSqlServer_ResolvesAllTransactionContractsAsSameScopedInstance()
    {
        var services = new ServiceCollection();
        services.AddScoped<ITestUnitOfWork, TestUnitOfWork>();
        services.AddAsyncomniaMessaging().UseInbox(inbox =>
            inbox.UseSqlServer<ITestUnitOfWork>(Configure));
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var unitOfWork = scope.ServiceProvider.GetRequiredService<ITestUnitOfWork>();
        var inboundTransaction = scope.ServiceProvider.GetRequiredService<IInboundTransaction>();
        var inboxTransaction = scope.ServiceProvider.GetRequiredService<IInboxTransaction>();
        var transactionAccessor = scope.ServiceProvider.GetRequiredService<IDbTransactionAccessor>();

        Assert.Same(unitOfWork, inboundTransaction);
        Assert.Same(unitOfWork, inboxTransaction);
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

    private static void AssertSingleFactoryRegistration<TService>(IServiceCollection services)
    {
        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(TService));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.NotNull(registration.ImplementationFactory);
    }
}
