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
    public void UseSqlServer_WithConnectionString_UsesDefaultTableLocation()
    {
        using var provider = CreateProvider(options => options.ConnectionString = "test-connection-string");

        var options = provider.GetRequiredService<IOptions<SqlServerOutboxOptions>>().Value;

        Assert.Equal("test-connection-string", options.ConnectionString);
        Assert.Equal("asyncomnia", options.SchemaName);
        Assert.Equal("Outbox", options.TableName);
    }

    [Fact]
    public void UseSqlServer_WithConfiguration_UsesConfiguredOptions()
    {
        using var provider = CreateProvider(options =>
        {
            options.ConnectionString = "configured-connection-string";
            options.SchemaName = "messaging";
            options.TableName = "OutboxMessages";
        });

        var options = provider.GetRequiredService<IOptions<SqlServerOutboxOptions>>().Value;

        Assert.Equal("configured-connection-string", options.ConnectionString);
        Assert.Equal("messaging", options.SchemaName);
        Assert.Equal("OutboxMessages", options.TableName);
    }

    [Fact]
    public void UseSqlServer_WhenConnectionStringIsEmpty_ThrowsValidationException()
    {
        var exception = GetValidationException(_ => { });

        Assert.Contains(
            "SqlServerOutboxOptions:ConnectionString cannot be empty",
            exception.Failures);
    }

    [Fact]
    public void UseSqlServer_WhenSchemaNameIsEmpty_ThrowsValidationException()
    {
        var exception = GetValidationException(options =>
        {
            options.ConnectionString = "test-connection-string";
            options.SchemaName = " ";
        });

        Assert.Contains(
            "SqlServerOutboxOptions:SchemaName cannot be empty",
            exception.Failures);
    }

    [Fact]
    public void UseSqlServer_WhenSchemaNameStartsWithDigit_ThrowsValidationException()
    {
        var exception = GetValidationException(options =>
        {
            options.ConnectionString = "test-connection-string";
            options.SchemaName = "1messaging";
        });

        Assert.Contains(
            "SqlServerOutboxOptions:SchemaName is not a valid SQL identifier",
            exception.Failures);
    }

    [Fact]
    public void UseSqlServer_WhenSchemaNameContainsInvalidCharacter_ThrowsValidationException()
    {
        var exception = GetValidationException(options =>
        {
            options.ConnectionString = "test-connection-string";
            options.SchemaName = "messaging-schema";
        });

        Assert.Contains(
            "SqlServerOutboxOptions:SchemaName is not a valid SQL identifier",
            exception.Failures);
    }

    [Fact]
    public void UseSqlServer_WhenTableNameIsEmpty_ThrowsValidationException()
    {
        var exception = GetValidationException(options =>
        {
            options.ConnectionString = "test-connection-string";
            options.TableName = " ";
        });

        Assert.Contains(
            "SqlServerOutboxOptions:TableName cannot be empty",
            exception.Failures);
    }

    [Fact]
    public void UseSqlServer_WhenTableNameIsInvalid_ThrowsValidationException()
    {
        var exception = GetValidationException(options =>
        {
            options.ConnectionString = "test-connection-string";
            options.TableName = "Outbox.Messages";
        });

        Assert.Contains(
            "SqlServerOutboxOptions:TableName is not a valid SQL identifier",
            exception.Failures);
    }

    private static ServiceProvider CreateProvider(Action<SqlServerOutboxOptions> configure)
    {
        var services = new ServiceCollection();
        services
            .AddAsyncomniaMessaging()
            .UseOutbox(outbox => outbox.UseSqlServer(configure));

        return services.BuildServiceProvider();
    }

    private static OptionsValidationException GetValidationException(Action<SqlServerOutboxOptions> configure)
    {
        using var provider = CreateProvider(configure);
        var options = provider.GetRequiredService<IOptions<SqlServerOutboxOptions>>();

        return Assert.Throws<OptionsValidationException>(() =>
        {
            _ = options.Value;
        });
    }
}
