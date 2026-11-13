using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Tests.TestDoubles;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.DependencyInjection;

public sealed class AdoNetMessagingBuilderExtensionsTests
{
    [Fact]
    public void UseAdoNet_ReturnsSameMessagingBuilder()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        var returnedBuilder = builder.UseAdoNet<ITestUnitOfWork>();

        Assert.Same(builder, returnedBuilder);
    }

    [Fact]
    public void UseAdoNet_CalledTwice_LeavesSingleWriterAndTransactionRegistrations()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseAdoNet<ITestUnitOfWork>();
        builder.UseAdoNet<ITestUnitOfWork>();

        AssertSingleScopedWriter(services);
        AssertSingleScopedDelayedMessageWriter(services);
        AssertSingleScopedFactory<IInboundTransaction>(services);
        AssertSingleScopedFactory<IDbTransactionAccessor>(services);
    }

    [Fact]
    public void UseAdoNet_AfterUseSqlServer_ReplacesStandaloneAccessorAndUsesSqlServerWriter()
    {
        var services = new ServiceCollection();
        var builder = services
            .AddAsyncomniaMessaging()
            .UseSqlServer(Configure);

        builder.UseAdoNet<ITestUnitOfWork>();

        AssertSingleScopedWriter(services);
        AssertSingleScopedDelayedMessageWriter(services);
        AssertSingleScopedFactory<IInboundTransaction>(services);
        AssertSingleScopedFactory<IDbTransactionAccessor>(services);
    }

    [Fact]
    public void UseAdoNet_ResolvesTransactionContractsAsSameScopedUnitOfWork()
    {
        var services = new ServiceCollection();
        services.AddScoped<ITestUnitOfWork, TestUnitOfWork>();
        services
            .AddAsyncomniaMessaging()
            .UseSqlServer(Configure)
            .UseAdoNet<ITestUnitOfWork>();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var unitOfWork = scope.ServiceProvider.GetRequiredService<ITestUnitOfWork>();
        var inboundTransaction = scope.ServiceProvider.GetRequiredService<IInboundTransaction>();
        var transactionAccessor = scope.ServiceProvider.GetRequiredService<IDbTransactionAccessor>();

        Assert.Same(unitOfWork, inboundTransaction);
        Assert.Same(unitOfWork, transactionAccessor);
    }

    private static void Configure(SqlServerOptions options)
    {
        options.ConnectionString = "test-connection-string";
    }

    private static void AssertSingleScopedWriter(IServiceCollection services)
    {
        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IOutboxWriter));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal(typeof(SqlServerOutboxWriter), registration.ImplementationType);
    }

    private static void AssertSingleScopedDelayedMessageWriter(IServiceCollection services)
    {
        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IDelayedMessageWriter));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal(
            typeof(SqlServerDelayedMessageWriter),
            registration.ImplementationType);
    }

    private static void AssertSingleScopedFactory<TService>(IServiceCollection services)
    {
        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(TService));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.NotNull(registration.ImplementationFactory);
    }
}
