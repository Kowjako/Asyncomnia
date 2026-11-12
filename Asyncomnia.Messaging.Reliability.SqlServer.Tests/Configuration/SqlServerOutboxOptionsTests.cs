using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.Configuration;

public sealed class SqlServerOutboxOptionsTests
{
    [Fact]
    public void UseSqlServer_WithConnectionString_UsesConfiguredConnectionString()
    {
        using var provider = CreateProvider(options => options.ConnectionString = "test-connection-string");

        var options = provider.GetRequiredService<IOptions<SqlServerOptions>>().Value;

        Assert.Equal("test-connection-string", options.ConnectionString);
    }

    [Fact]
    public void UseSqlServer_WhenConnectionStringIsEmpty_ThrowsValidationException()
    {
        var exception = GetValidationException(_ => { });

        Assert.Contains(
            "SqlServerOutboxOptions:ConnectionString cannot be empty",
            exception.Failures);
    }

    private static ServiceProvider CreateProvider(Action<SqlServerOptions> configure)
    {
        var services = new ServiceCollection();
        services
            .AddAsyncomniaMessaging()
            .UseOutbox(outbox => outbox.UseSqlServer(configure));

        return services.BuildServiceProvider();
    }

    private static OptionsValidationException GetValidationException(Action<SqlServerOptions> configure)
    {
        using var provider = CreateProvider(configure);
        var options = provider.GetRequiredService<IOptions<SqlServerOptions>>();

        return Assert.Throws<OptionsValidationException>(() =>
        {
            _ = options.Value;
        });
    }
}
