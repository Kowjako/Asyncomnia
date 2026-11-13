using System.Data;
using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.TestDoubles;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.DependencyInjection;

public sealed class EntityFrameworkCoreMessagingBuilderExtensionsTests
{
    [Fact]
    public void UseEntityFrameworkCore_ReturnsSameMessagingBuilder()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        var returnedBuilder = builder.UseEntityFrameworkCore<TestDbContext>();

        Assert.Same(builder, returnedBuilder);
    }

    [Fact]
    public void UseEntityFrameworkCore_CalledTwice_LeavesSingleWriterAndTransactionRegistrations()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseEntityFrameworkCore<TestDbContext>();
        builder.UseEntityFrameworkCore<TestDbContext>();

        AssertEntityFrameworkCoreWriter(services);
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType ==
                typeof(EntityFrameworkCoreInboundTransaction<TestDbContext>));
        AssertSingleScopedFactory<IInboundTransaction>(services);
        AssertSingleScopedFactory<IDbTransactionAccessor>(services);
    }

    [Fact]
    public void UseEntityFrameworkCore_AfterUseSqlServer_ReplacesDefaultSqlServerWriter()
    {
        var services = new ServiceCollection();
        var builder = services
            .AddAsyncomniaMessaging()
            .UseSqlServer(Configure);

        builder.UseEntityFrameworkCore<TestDbContext>();

        AssertEntityFrameworkCoreWriter(services);
        AssertSingleScopedFactory<IInboundTransaction>(services);
        AssertSingleScopedFactory<IDbTransactionAccessor>(services);
    }

    [Fact]
    public void UseSqlServer_AfterUseEntityFrameworkCore_DoesNotReplaceEntityFrameworkCoreServices()
    {
        var services = new ServiceCollection();
        var builder = services
            .AddAsyncomniaMessaging()
            .UseEntityFrameworkCore<TestDbContext>();

        builder.UseSqlServer(Configure);

        AssertEntityFrameworkCoreWriter(services);
        AssertSingleScopedFactory<IInboundTransaction>(services);
        AssertSingleScopedFactory<IDbTransactionAccessor>(services);
    }

    [Fact]
    public void UseAdoNet_AfterUseEntityFrameworkCore_ReplacesWriterAndTransactionServices()
    {
        var services = new ServiceCollection();
        var builder = services
            .AddAsyncomniaMessaging()
            .UseSqlServer(Configure)
            .UseEntityFrameworkCore<TestDbContext>();

        builder.UseAdoNet<TestInboundTransaction>();

        var writerRegistration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IOutboxWriter));
        Assert.Equal("SqlServerOutboxWriter", writerRegistration.ImplementationType?.Name);
        AssertSingleScopedFactory<IInboundTransaction>(services);
        AssertSingleScopedFactory<IDbTransactionAccessor>(services);
    }

    [Fact]
    public void UseEntityFrameworkCore_ResolvesTransactionContractsAsSameScopedAdapter()
    {
        var services = new ServiceCollection();
        services.AddDbContext<TestDbContext>(options =>
            options.UseInMemoryDatabase(Guid.NewGuid().ToString()));
        services
            .AddAsyncomniaMessaging()
            .UseSqlServer(Configure)
            .UseEntityFrameworkCore<TestDbContext>();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var inboundTransaction = scope.ServiceProvider.GetRequiredService<IInboundTransaction>();
        var transactionAccessor = scope.ServiceProvider.GetRequiredService<IDbTransactionAccessor>();

        Assert.Same(inboundTransaction, transactionAccessor);
        Assert.IsType<EntityFrameworkCoreInboundTransaction<TestDbContext>>(inboundTransaction);
    }

    private static void Configure(SqlServerOptions options)
    {
        options.ConnectionString = "test-connection-string";
    }

    private static void AssertEntityFrameworkCoreWriter(IServiceCollection services)
    {
        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IOutboxWriter));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal(
            typeof(EntityFrameworkCoreOutboxWriter<TestDbContext>),
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

    private sealed class TestInboundTransaction : IInboundTransaction, IDbTransactionAccessor
    {
        public IDbTransaction? CurrentTransaction => null;

        public Task BeginAsync(CancellationToken token = default) => Task.CompletedTask;

        public Task CommitAsync(CancellationToken token = default) => Task.CompletedTask;

        public Task RollbackAsync(CancellationToken token = default) => Task.CompletedTask;
    }
}
