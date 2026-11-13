using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DelayedPublishing.Configuration;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Tests.DelayedPublishing;

public sealed class DelayedPublishingOptionsTests
{
    [Fact]
    public void UseDelayedPublishing_WithoutConfiguration_UsesDefaultOptions()
    {
        using var provider = CreateProvider();

        var options = provider
            .GetRequiredService<IOptions<DelayedPublishingOptions>>()
            .Value;

        Assert.Equal(100, options.BatchSize);
        Assert.Equal(TimeSpan.FromSeconds(1), options.PollingInterval);
    }

    [Fact]
    public void UseDelayedPublishing_WithConfiguration_UsesConfiguredOptions()
    {
        using var provider = CreateProvider(options =>
        {
            options.BatchSize = 37;
            options.PollingInterval = TimeSpan.FromSeconds(4);
        });

        var options = provider
            .GetRequiredService<IOptions<DelayedPublishingOptions>>()
            .Value;

        Assert.Equal(37, options.BatchSize);
        Assert.Equal(TimeSpan.FromSeconds(4), options.PollingInterval);
    }

    [Fact]
    public void UseDelayedPublishing_WhenBatchSizeIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options => options.BatchSize = 0);

        Assert.Contains(
            "DelayedPublishingOptions:BatchSize must be greater than zero",
            exception.Failures);
    }

    [Fact]
    public void UseDelayedPublishing_WhenPollingIntervalIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options =>
            options.PollingInterval = TimeSpan.Zero);

        Assert.Contains(
            "DelayedPublishingOptions:PollingInterval must be greater than zero",
            exception.Failures);
    }

    private static ServiceProvider CreateProvider(
        Action<DelayedPublishingOptions>? configure = null)
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        if (configure is null)
        {
            builder.UseDelayedPublishing();
        }
        else
        {
            builder.UseDelayedPublishing(delayed => delayed.Configure(configure));
        }

        return services.BuildServiceProvider();
    }

    private static OptionsValidationException GetValidationException(
        Action<DelayedPublishingOptions> configure)
    {
        using var provider = CreateProvider(configure);
        var options = provider.GetRequiredService<IOptions<DelayedPublishingOptions>>();

        return Assert.Throws<OptionsValidationException>(() =>
        {
            _ = options.Value;
        });
    }
}
