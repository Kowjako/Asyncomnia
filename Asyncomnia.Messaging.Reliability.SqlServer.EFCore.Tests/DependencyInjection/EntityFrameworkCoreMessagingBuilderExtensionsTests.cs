using System.Data;
using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Infrastructure;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.TestDoubles;
using Asyncomnia.Messaging.Reliability.SqlServer.Infrastructure.Connection;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.DependencyInjection;

public sealed class EntityFrameworkCoreMessagingBuilderExtensionsTests
{
    [Fact]
    public void UseEntityFrameworkCore_WhenBuilderIsNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            EntityFrameworkCoreMessagingBuilderExtensions
                .UseEntityFrameworkCore<TestDbContext>(null!));
    }

    [Fact]
    public void UseEntityFrameworkCore_ReturnsSameSqlServerBuilder()
    {
        var services = new ServiceCollection();
        SqlServerBuilder? configuredBuilder = null;

        services.AddAsyncomniaMessaging().UseSqlServer(sql =>
        {
            configuredBuilder = sql;

            var returnedBuilder = sql.UseEntityFrameworkCore<TestDbContext>();

            Assert.Same(sql, returnedBuilder);
        });

        Assert.NotNull(configuredBuilder);
    }

    [Fact]
    public void UseEntityFrameworkCore_CalledTwice_LeavesSingleAdapterRegistrationSet()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging().UseSqlServer(sql =>
        {
            sql.UseEntityFrameworkCore<TestDbContext>();
            sql.UseEntityFrameworkCore<TestDbContext>();
        });

        AssertEntityFrameworkCoreWriter(services);
        AssertEntityFrameworkCoreDelayedMessageWriter(services);
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType ==
                typeof(EntityFrameworkCoreInboundTransaction<TestDbContext>));
        AssertSingleScopedFactory<IInboundTransaction>(services);
        AssertSingleScopedFactory<IDbTransactionAccessor>(services);
        AssertSingleScopedRegistration<ISqlServerConnectionFactory>(
            services,
            typeof(EntityFrameworkCoreSqlServerConnectionFactory<TestDbContext>));
    }

    [Fact]
    public void UseEntityFrameworkCore_WithDifferentDbContextsAndNoExplicitConnectionString_ThrowsInvalidOperationException()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddAsyncomniaMessaging().UseSqlServer(sql =>
            {
                sql.UseEntityFrameworkCore<TestDbContext>();
                sql.UseEntityFrameworkCore<UnconfiguredDbContext>();
            }));

        Assert.Contains("connection source has already been configured", exception.Message);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void UseConnectionString_AndUseEntityFrameworkCore_ExplicitConnectionStringWins(
        bool connectionStringFirst)
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging().UseSqlServer(sql =>
        {
            if (connectionStringFirst)
            {
                sql.UseConnectionString("Server=localhost;Database=AsyncomniaTests");
                sql.UseEntityFrameworkCore<TestDbContext>();
            }
            else
            {
                sql.UseEntityFrameworkCore<TestDbContext>();
                sql.UseConnectionString("Server=localhost;Database=AsyncomniaTests");
            }
        });

        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(ISqlServerConnectionFactory));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.IsType<ConnectionStringSqlServerConnectionFactory>(registration.ImplementationInstance);
        AssertEntityFrameworkCoreWriter(services);
        AssertEntityFrameworkCoreDelayedMessageWriter(services);
    }

    [Fact]
    public void UseAdoNet_AfterUseEntityFrameworkCore_ReplacesWriterAndTransactionServices()
    {
        var services = new ServiceCollection();

        services.AddAsyncomniaMessaging().UseSqlServer(sql =>
        {
            sql.UseEntityFrameworkCore<TestDbContext>();
            sql.UseAdoNet<TestInboundTransaction>();
        });

        var writerRegistration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IOutboxWriter));
        Assert.Equal("SqlServerOutboxWriter", writerRegistration.ImplementationType?.Name);

        var delayedWriterRegistration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IDelayedMessageWriter));
        Assert.Equal(
            "SqlServerDelayedMessageWriter",
            delayedWriterRegistration.ImplementationType?.Name);
        AssertSingleScopedFactory<IInboundTransaction>(services);
        AssertSingleScopedFactory<IDbTransactionAccessor>(services);
    }

    [Fact]
    public void UseEntityFrameworkCore_ResolvesTransactionContractsAsSameScopedAdapter()
    {
        var services = new ServiceCollection();
        services.AddDbContext<TestDbContext>(options =>
            options.UseInMemoryDatabase(Guid.NewGuid().ToString()));
        services.AddAsyncomniaMessaging().UseSqlServer(sql =>
        {
            sql.UseConnectionString("Server=localhost;Database=AsyncomniaTests");
            sql.UseEntityFrameworkCore<TestDbContext>();
        });
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var inboundTransaction = scope.ServiceProvider.GetRequiredService<IInboundTransaction>();
        var transactionAccessor = scope.ServiceProvider.GetRequiredService<IDbTransactionAccessor>();

        Assert.Same(inboundTransaction, transactionAccessor);
        Assert.IsType<EntityFrameworkCoreInboundTransaction<TestDbContext>>(inboundTransaction);
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

    private static void AssertEntityFrameworkCoreDelayedMessageWriter(
        IServiceCollection services)
    {
        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IDelayedMessageWriter));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal(
            typeof(EntityFrameworkCoreDelayedMessageWriter<TestDbContext>),
            registration.ImplementationType);
    }

    private static void AssertSingleScopedRegistration<TService>(
        IServiceCollection services,
        Type implementationType)
    {
        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(TService));
        Assert.Equal(ServiceLifetime.Scoped, registration.Lifetime);
        Assert.Equal(implementationType, registration.ImplementationType);
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
