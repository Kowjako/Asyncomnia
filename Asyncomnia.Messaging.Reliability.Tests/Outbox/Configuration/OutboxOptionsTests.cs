using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Tests.Outbox.Configuration;

public sealed class OutboxOptionsTests
{
    [Fact]
    public void UseOutbox_WithoutConfiguration_UsesDefaultOptions()
    {
        using var provider = CreateProvider();

        var options = provider.GetRequiredService<IOptions<OutboxOptions>>().Value;

        Assert.Equal(1, options.WorkerCount);
        Assert.Equal(10, options.BatchSize);
        Assert.Equal(TimeSpan.FromSeconds(1), options.PollingInterval);
        Assert.Equal(TimeSpan.FromMinutes(1), options.LeaseDuration);
        Assert.Equal(TimeSpan.FromSeconds(5), options.RetryDelay);
        Assert.Equal(10, options.MaxAttempts);
    }

    [Fact]
    public void UseOutbox_WithConfiguration_UsesConfiguredOptions()
    {
        using var provider = CreateProvider(options =>
        {
            options.WorkerCount = 4;
            options.BatchSize = 20;
            options.PollingInterval = TimeSpan.FromMilliseconds(500);
            options.LeaseDuration = TimeSpan.FromMinutes(2);
            options.RetryDelay = TimeSpan.FromSeconds(9);
            options.MaxAttempts = 5;
        });

        var options = provider.GetRequiredService<IOptions<OutboxOptions>>().Value;

        Assert.Equal(4, options.WorkerCount);
        Assert.Equal(20, options.BatchSize);
        Assert.Equal(TimeSpan.FromMilliseconds(500), options.PollingInterval);
        Assert.Equal(TimeSpan.FromMinutes(2), options.LeaseDuration);
        Assert.Equal(TimeSpan.FromSeconds(9), options.RetryDelay);
        Assert.Equal(5, options.MaxAttempts);
    }

    [Fact]
    public void UseOutbox_WhenWorkerCountIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options => options.WorkerCount = 0);

        Assert.Contains(
            "OutboxOptions:WorkerCount must be greater than zero",
            exception.Failures);
    }

    [Fact]
    public void UseOutbox_WhenBatchSizeIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options => options.BatchSize = 0);

        Assert.Contains(
            "OutboxOptions:BatchSize must be greater than zero",
            exception.Failures);
    }

    [Fact]
    public void UseOutbox_WhenPollingIntervalIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options => options.PollingInterval = TimeSpan.Zero);

        Assert.Contains(
            "OutboxOptions:PollingInterval must be greater than zero",
            exception.Failures);
    }

    [Fact]
    public void UseOutbox_WhenLeaseDurationIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options => options.LeaseDuration = TimeSpan.Zero);

        Assert.Contains(
            "OutboxOptions:LeaseDuration must be greater than zero",
            exception.Failures);
    }

    [Fact]
    public void UseOutbox_WhenMaxAttemptsIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options => options.MaxAttempts = 0);

        Assert.Contains(
            "OutboxOptions:MaxAttempts must be greater than zero",
            exception.Failures);
    }

    [Fact]
    public void UseOutbox_WhenRetryDelayIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options => options.RetryDelay = TimeSpan.Zero);

        Assert.Contains(
            "OutboxOptions:RetryDelay must be greater than zero",
            exception.Failures);
    }

    private static ServiceProvider CreateProvider(Action<OutboxOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddAsyncomniaMessaging().UseOutbox(configure);

        return services.BuildServiceProvider();
    }

    private static OptionsValidationException GetValidationException(Action<OutboxOptions> configure)
    {
        using var provider = CreateProvider(configure);
        var options = provider.GetRequiredService<IOptions<OutboxOptions>>();

        return Assert.Throws<OptionsValidationException>(() =>
        {
            _ = options.Value;
        });
    }
}
