using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Infrastructure.Connection;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.Configuration;

public sealed class SqlServerBuilderTests
{
    [Fact]
    public void Constructor_WhenServicesAreNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new SqlServerBuilder(null!));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void UseConnectionString_WhenConnectionStringIsInvalid_ThrowsArgumentException(
        string? connectionString)
    {
        var builder = new SqlServerBuilder(new ServiceCollection());

        Assert.ThrowsAny<ArgumentException>(() =>
            builder.UseConnectionString(connectionString!));
    }

    [Fact]
    public void EnsureConnectionSourceConfigured_WhenSourceIsMissing_ThrowsInvalidOperationException()
    {
        var builder = new SqlServerBuilder(new ServiceCollection());

        var exception = Assert.Throws<InvalidOperationException>(() =>
            builder.EnsureConnectionSourceConfigured());

        Assert.Contains("UseConnectionString", exception.Message);
        Assert.Contains("UseEntityFrameworkCore", exception.Message);
    }

    [Fact]
    public void UseConnectionString_RegistersSingleConnectionFactory()
    {
        var services = new ServiceCollection();
        var builder = new SqlServerBuilder(services);

        builder.UseConnectionString("Server=localhost;Database=AsyncomniaTests");
        builder.UseConnectionString("Server=localhost;Database=AsyncomniaTests2");

        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(ISqlServerConnectionFactory));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.IsType<ConnectionStringSqlServerConnectionFactory>(registration.ImplementationInstance);
    }

    [Fact]
    public void TryUseConnectionFactory_WhenDifferentSourceWasAlreadyConfigured_ThrowsInvalidOperationException()
    {
        var builder = new SqlServerBuilder(new ServiceCollection());
        builder.TryUseConnectionFactory<FirstConnectionFactory>();

        Assert.Throws<InvalidOperationException>(() =>
            builder.TryUseConnectionFactory<SecondConnectionFactory>());
    }

    [Fact]
    public void UseConnectionString_AfterAdapter_ReplacesAdapterConnectionFactory()
    {
        var services = new ServiceCollection();
        var builder = new SqlServerBuilder(services);
        builder.TryUseConnectionFactory<FirstConnectionFactory>();

        builder = new SqlServerBuilder(services);
        builder.UseConnectionString("Server=localhost;Database=AsyncomniaTests");

        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(ISqlServerConnectionFactory));
        Assert.IsType<ConnectionStringSqlServerConnectionFactory>(registration.ImplementationInstance);
    }

    [Fact]
    public void TryUseConnectionFactory_AfterExplicitConnectionString_DoesNotReplaceExplicitSource()
    {
        var services = new ServiceCollection();
        var builder = new SqlServerBuilder(services);
        builder.UseConnectionString("Server=localhost;Database=AsyncomniaTests");

        builder = new SqlServerBuilder(services);
        builder.TryUseConnectionFactory<FirstConnectionFactory>();

        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(ISqlServerConnectionFactory));
        Assert.IsType<ConnectionStringSqlServerConnectionFactory>(registration.ImplementationInstance);
    }

    [Fact]
    public void TryUseConnectionFactory_FromNewBuilderWithSameAdapter_IsIdempotent()
    {
        var services = new ServiceCollection();
        var builder = new SqlServerBuilder(services);
        builder.TryUseConnectionFactory<FirstConnectionFactory>();

        builder = new SqlServerBuilder(services);
        builder.TryUseConnectionFactory<FirstConnectionFactory>();

        var registration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(ISqlServerConnectionFactory));
        Assert.Equal(typeof(FirstConnectionFactory), registration.ImplementationType);
    }

    [Fact]
    public void TryUseConnectionFactory_FromNewBuilderWithDifferentAdapter_ThrowsInvalidOperationException()
    {
        var services = new ServiceCollection();
        var builder = new SqlServerBuilder(services);
        builder.TryUseConnectionFactory<FirstConnectionFactory>();

        builder = new SqlServerBuilder(services);

        Assert.Throws<InvalidOperationException>(() =>
            builder.TryUseConnectionFactory<SecondConnectionFactory>());
    }

    private sealed class FirstConnectionFactory : ISqlServerConnectionFactory
    {
        public ValueTask<SqlConnection> OpenConnectionAsync(CancellationToken token = default) =>
            throw new NotSupportedException();
    }

    private sealed class SecondConnectionFactory : ISqlServerConnectionFactory
    {
        public ValueTask<SqlConnection> OpenConnectionAsync(CancellationToken token = default) =>
            throw new NotSupportedException();
    }
}
