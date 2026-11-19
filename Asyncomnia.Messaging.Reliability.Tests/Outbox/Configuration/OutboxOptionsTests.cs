using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;

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
        Assert.True(options.CleanupEnabled);
        Assert.Equal(TimeSpan.FromDays(7), options.ProcessedMessageRetention);
        Assert.Null(options.FailedMessageRetention);
        Assert.Equal(1000, options.CleanupBatchSize);
        Assert.Equal(TimeSpan.FromMinutes(1), options.CleanupInterval);
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
            options.ConfigureRetryPolicy(
                5,
                TimeSpan.FromSeconds(9));
            options.ConfigureCleanup(
                TimeSpan.FromDays(3),
                TimeSpan.FromDays(30),
                TimeSpan.FromMinutes(5),
                250);
            options.DisableCleanup();
        });

        var options = provider.GetRequiredService<IOptions<OutboxOptions>>().Value;

        Assert.Equal(4, options.WorkerCount);
        Assert.Equal(20, options.BatchSize);
        Assert.Equal(TimeSpan.FromMilliseconds(500), options.PollingInterval);
        Assert.Equal(TimeSpan.FromMinutes(2), options.LeaseDuration);
        Assert.Equal(TimeSpan.FromSeconds(9), options.RetryDelay);
        Assert.Equal(5, options.MaxAttempts);
        Assert.False(options.CleanupEnabled);
        Assert.Equal(TimeSpan.FromDays(3), options.ProcessedMessageRetention);
        Assert.Equal(TimeSpan.FromDays(30), options.FailedMessageRetention);
        Assert.Equal(250, options.CleanupBatchSize);
        Assert.Equal(TimeSpan.FromMinutes(5), options.CleanupInterval);
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
        var exception = GetValidationException(options =>
            options.ConfigureRetryPolicy(0, options.RetryDelay));

        Assert.Contains(
            "OutboxOptions:MaxAttempts must be greater than zero",
            exception.Failures);
    }

    [Fact]
    public void UseOutbox_WhenRetryDelayIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options =>
            options.ConfigureRetryPolicy(options.MaxAttempts, TimeSpan.Zero));

        Assert.Contains(
            "OutboxOptions:RetryDelay must be greater than zero",
            exception.Failures);
    }

    [Fact]
    public void UseOutbox_WhenProcessedMessageRetentionIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options =>
            options.ConfigureCleanup(
                TimeSpan.Zero,
                options.FailedMessageRetention,
                options.CleanupInterval,
                options.CleanupBatchSize));

        Assert.Contains(
            "OutboxOptions:ProcessedMessageRetention must be greater than zero",
            exception.Failures);
    }

    [Fact]
    public void UseOutbox_WhenFailedMessageRetentionIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options =>
            options.ConfigureCleanup(
                options.ProcessedMessageRetention,
                TimeSpan.Zero,
                options.CleanupInterval,
                options.CleanupBatchSize));

        Assert.Contains(
            "OutboxOptions:FailedMessageRetention must be greater than zero when configured",
            exception.Failures);
    }

    [Fact]
    public void UseOutbox_WhenCleanupBatchSizeIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options =>
            options.ConfigureCleanup(
                options.ProcessedMessageRetention,
                options.FailedMessageRetention,
                options.CleanupInterval,
                0));

        Assert.Contains(
            "OutboxOptions:CleanupBatchSize must be greater than zero",
            exception.Failures);
    }

    [Fact]
    public void UseOutbox_WhenCleanupIntervalIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options =>
            options.ConfigureCleanup(
                options.ProcessedMessageRetention,
                options.FailedMessageRetention,
                TimeSpan.Zero,
                options.CleanupBatchSize));

        Assert.Contains(
            "OutboxOptions:CleanupInterval must be greater than zero",
            exception.Failures);
    }

    [Fact]
    public void UseOutbox_WithoutStorageProvider_ThrowsValidationException()
    {
        using var provider = CreateProvider(
            registerOutboxStore: false,
            registerCleanupStore: false);
        var options = provider.GetRequiredService<IOptions<OutboxOptions>>();

        var exception = Assert.Throws<OptionsValidationException>(() =>
        {
            _ = options.Value;
        });

        Assert.Contains(
            "Outbox storage is not configured. Register a storage provider, for example by calling UseSqlServer(...).",
            exception.Failures);
    }

    [Fact]
    public void UseOutbox_WithCleanupEnabledAndWithoutCleanupStore_ThrowsValidationException()
    {
        using var provider = CreateProvider(registerCleanupStore: false);
        var options = provider.GetRequiredService<IOptions<OutboxOptions>>();

        var exception = Assert.Throws<OptionsValidationException>(() =>
        {
            _ = options.Value;
        });

        Assert.Contains(
            "Outbox cleanup is enabled, but the configured storage provider does not support it. " +
            "Register an IOutboxCleanupStore implementation or disable outbox cleanup.",
            exception.Failures);
    }

    [Fact]
    public void UseOutbox_WithCleanupDisabled_DoesNotRequireCleanupStore()
    {
        using var provider = CreateProvider(
            options => options.DisableCleanup(),
            registerCleanupStore: false);

        var options = provider.GetRequiredService<IOptions<OutboxOptions>>().Value;

        Assert.False(options.CleanupEnabled);
    }

    [Fact]
    public void ConfigureRetryPolicy_SetsConfiguration()
    {
        var options = new OutboxOptions();

        options.ConfigureRetryPolicy(
            17,
            TimeSpan.FromSeconds(23));

        Assert.Equal(17, options.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(23), options.RetryDelay);
    }

    [Fact]
    public void ConfigureCleanup_EnablesCleanupAndSetsConfiguration()
    {
        var options = new OutboxOptions();
        options.DisableCleanup();

        options.ConfigureCleanup(
            TimeSpan.FromDays(2),
            TimeSpan.FromDays(11),
            TimeSpan.FromMinutes(7),
            456);

        Assert.True(options.CleanupEnabled);
        Assert.Equal(TimeSpan.FromDays(2), options.ProcessedMessageRetention);
        Assert.Equal(TimeSpan.FromDays(11), options.FailedMessageRetention);
        Assert.Equal(TimeSpan.FromMinutes(7), options.CleanupInterval);
        Assert.Equal(456, options.CleanupBatchSize);
    }

    [Fact]
    public void ConfigureCleanup_WithNullFailedRetention_PreservesNull()
    {
        var options = new OutboxOptions();

        options.ConfigureCleanup(
            TimeSpan.FromDays(2),
            null,
            TimeSpan.FromMinutes(7),
            456);

        Assert.Null(options.FailedMessageRetention);
    }

    [Fact]
    public void DisableCleanup_DisablesCleanup()
    {
        var options = new OutboxOptions();

        options.DisableCleanup();

        Assert.False(options.CleanupEnabled);
    }

    private static ServiceProvider CreateProvider(
        Action<OutboxOptions>? configure = null,
        bool registerOutboxStore = true,
        bool registerCleanupStore = true)
    {
        var services = new ServiceCollection();

        if (registerOutboxStore)
        {
            services.AddSingleton(Mock.Of<IOutboxStore>());
        }

        if (registerCleanupStore)
        {
            services.AddSingleton(Mock.Of<IOutboxCleanupStore>());
        }

        var builder = services.AddAsyncomniaMessaging();

        if (configure is null)
        {
            builder.UseOutbox();
        }
        else
        {
            builder.UseOutbox(configure);
        }

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
