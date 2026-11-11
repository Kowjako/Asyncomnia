using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Tests.TestDoubles;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Transactions;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Tests.DependencyInjection;

public sealed class DapperOutboxBuilderExtensionsTests
{
    [Fact]
    public void UseDapper_ReturnsSameOutboxBuilder()
    {
        var services = new ServiceCollection();
        OutboxBuilder? configuredBuilder = null;
        OutboxBuilder? returnedBuilder = null;

        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
        {
            configuredBuilder = outbox;
            returnedBuilder = outbox.UseDapper<ITestUnitOfWork>();
        });

        Assert.Same(configuredBuilder, returnedBuilder);
    }

    [Fact]
    public void UseDapper_CalledTwice_LeavesSingleScopedTransactionAccessorRegistration()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
        {
            outbox.UseDapper<ITestUnitOfWork>();
            outbox.UseDapper<ITestUnitOfWork>();
        });

        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IDbTransactionAccessor));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.NotNull(registration.ImplementationFactory);
    }

    [Fact]
    public void UseDapper_CalledTwice_LeavesSingleScopedWriterRegistration()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
        {
            outbox.UseDapper<ITestUnitOfWork>();
            outbox.UseDapper<ITestUnitOfWork>();
        });

        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IOutboxWriter));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal(typeof(DapperOutboxWriter), registration.ImplementationType);
    }

    [Fact]
    public void UseDapper_WithInterfaceAccessor_ResolvesSameScopedInstance()
    {
        var services = new ServiceCollection();
        services.AddScoped<ITestUnitOfWork, TestUnitOfWork>();
        services.AddAsyncomniaMessaging().UseOutbox(outbox =>
            outbox.UseDapper<ITestUnitOfWork>());
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var unitOfWork = scope.ServiceProvider.GetRequiredService<ITestUnitOfWork>();
        var transactionAccessor = scope.ServiceProvider.GetRequiredService<IDbTransactionAccessor>();

        Assert.Same(unitOfWork, transactionAccessor);
    }
}
