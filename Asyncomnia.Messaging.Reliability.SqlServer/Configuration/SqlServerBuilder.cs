using Asyncomnia.Messaging.Reliability.SqlServer.Infrastructure.Connection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Configuration;

public sealed class SqlServerBuilder
{
    private bool _connectionSourceConfigured;
    private bool _explicitConnectionStringConfigured;
    private Type? _connectionFactoryType;

    public IServiceCollection Services { get; }

    internal SqlServerBuilder(IServiceCollection services)
    {
        Services = services ?? throw new ArgumentNullException(nameof(services));

        var connectionFactoryRegistration = Services.LastOrDefault(descriptor => descriptor.ServiceType == typeof(ISqlServerConnectionFactory));

        if (connectionFactoryRegistration is null)
        {
            return;
        }

        _connectionSourceConfigured = true;
        _explicitConnectionStringConfigured = 
            connectionFactoryRegistration.ImplementationInstance is ConnectionStringSqlServerConnectionFactory;
        _connectionFactoryType = connectionFactoryRegistration.ImplementationType;
    }

    public void UseConnectionString(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        Services.RemoveAll<ISqlServerConnectionFactory>();

        Services.AddSingleton<ISqlServerConnectionFactory>(new ConnectionStringSqlServerConnectionFactory(connectionString));

        _connectionSourceConfigured = true;
        _explicitConnectionStringConfigured = true;
        _connectionFactoryType = null;
    }

    internal void TryUseConnectionFactory<TFactory>() where TFactory : class, ISqlServerConnectionFactory
    {
        if (_explicitConnectionStringConfigured)
        {
            return;
        }

        if (_connectionSourceConfigured)
        {
            if (_connectionFactoryType == typeof(TFactory))
            {
                return;
            }

            throw new InvalidOperationException("SQL Server connection source has already been configured.");
        }

        Services.TryAddScoped<ISqlServerConnectionFactory, TFactory>();

        _connectionSourceConfigured = true;
        _connectionFactoryType = typeof(TFactory);
    }

    internal void EnsureConnectionSourceConfigured()
    {
        if (!_connectionSourceConfigured)
        {
            throw new InvalidOperationException("SQL Server connection source is not configured. Call UseConnectionString(...) or configure an adapter such as UseEntityFrameworkCore<TDbContext>().");
        }
    }
}
