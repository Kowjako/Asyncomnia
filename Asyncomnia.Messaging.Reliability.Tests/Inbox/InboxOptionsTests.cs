using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Inbox.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Tests.Inbox;

public sealed class InboxOptionsTests
{
    [Fact]
    public void UseInbox_WithoutConfiguration_UsesDefaultOptions()
    {
        using var provider = CreateProvider();

        var options = provider.GetRequiredService<IOptions<InboxOptions>>().Value;

        Assert.Equal(TimeSpan.FromMinutes(30), options.DuplicateDetectionWindow);
        Assert.Equal(100, options.CleanupBatchSize);
        Assert.Equal(TimeSpan.FromSeconds(10), options.CleanupInterval);
        Assert.True(options.CleanupEnabled);
    }

    [Fact]
    public void UseInbox_WithConfiguration_UsesConfiguredOptions()
    {
        using var provider = CreateProvider(options =>
        {
            options.DuplicateDetectionWindow = TimeSpan.FromHours(2);
            options.CleanupBatchSize = 250;
            options.CleanupInterval = TimeSpan.FromMinutes(1);
            options.CleanupEnabled = false;
        });

        var options = provider.GetRequiredService<IOptions<InboxOptions>>().Value;

        Assert.Equal(TimeSpan.FromHours(2), options.DuplicateDetectionWindow);
        Assert.Equal(250, options.CleanupBatchSize);
        Assert.Equal(TimeSpan.FromMinutes(1), options.CleanupInterval);
        Assert.False(options.CleanupEnabled);
    }

    [Fact]
    public void UseInbox_WhenDuplicateDetectionWindowIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options =>
            options.DuplicateDetectionWindow = TimeSpan.Zero);

        Assert.Contains(
            "InboxOptions:DuplicateDetectionWindow must be greater than zero",
            exception.Failures);
    }

    [Fact]
    public void UseInbox_WhenCleanupBatchSizeIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options =>
            options.CleanupBatchSize = 0);

        Assert.Contains(
            "InboxOptions:CleanupBatchSize must be greater than zero",
            exception.Failures);
    }

    [Fact]
    public void UseInbox_WhenCleanupIntervalIsNotPositive_ThrowsValidationException()
    {
        var exception = GetValidationException(options =>
            options.CleanupInterval = TimeSpan.Zero);

        Assert.Contains(
            "InboxOptions:CleanupInterval must be greater than zero",
            exception.Failures);
    }

    private static ServiceProvider CreateProvider(Action<InboxOptions>? configure = null)
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        if (configure is null)
        {
            builder.UseInbox();
        }
        else
        {
            builder.UseInbox(inbox => inbox.Configure(configure));
        }

        return services.BuildServiceProvider();
    }

    private static OptionsValidationException GetValidationException(
        Action<InboxOptions> configure)
    {
        using var provider = CreateProvider(configure);
        var options = provider.GetRequiredService<IOptions<InboxOptions>>();

        return Assert.Throws<OptionsValidationException>(() =>
        {
            _ = options.Value;
        });
    }
}
