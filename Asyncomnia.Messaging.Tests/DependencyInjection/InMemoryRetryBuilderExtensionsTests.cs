using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Retry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Tests.DependencyInjection;

public sealed class InMemoryRetryBuilderExtensionsTests
{
    [Fact]
    public void UseInMemoryRetries_ConfiguresImmediateRetryStrategy()
    {
        var services = new ServiceCollection();
        services
            .AddAsyncomniaMessaging()
            .UseInMemoryRetries(options => options.Immediately(2));
        using var provider = services.BuildServiceProvider();

        var options = provider
            .GetRequiredService<IOptions<InMemoryRetryOptions>>()
            .Value;

        Assert.Equal([TimeSpan.Zero, TimeSpan.Zero], options.Delays);
    }

    [Fact]
    public void UseInMemoryRetries_ConfiguresIntervalRetryStrategy()
    {
        var services = new ServiceCollection();
        var firstDelay = TimeSpan.FromMilliseconds(100);
        var secondDelay = TimeSpan.FromMilliseconds(500);
        services
            .AddAsyncomniaMessaging()
            .UseInMemoryRetries(options =>
                options.Intervals(firstDelay, secondDelay));
        using var provider = services.BuildServiceProvider();

        var options = provider
            .GetRequiredService<IOptions<InMemoryRetryOptions>>()
            .Value;

        Assert.Equal([firstDelay, secondDelay], options.Delays);
    }

    [Fact]
    public void UseInMemoryRetries_WithoutConfiguredStrategy_FailsOptionsValidation()
    {
        var services = new ServiceCollection();
        services
            .AddAsyncomniaMessaging()
            .UseInMemoryRetries(_ => { });
        using var provider = services.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider
            .GetRequiredService<IOptions<InMemoryRetryOptions>>()
            .Value);
    }

    [Fact]
    public void UseInMemoryRetries_RegistersValidatorOnlyOnce()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseInMemoryRetries(options => options.Immediately(1));
        builder.UseInMemoryRetries(options => options.Immediately(1));

        Assert.Single(
            services,
            descriptor => descriptor.ServiceType ==
                typeof(IValidateOptions<InMemoryRetryOptions>));
    }

    [Fact]
    public void UseInMemoryRetries_WhenConfigureIsNull_Throws()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        Assert.Throws<ArgumentNullException>(
            () => builder.UseInMemoryRetries(null!));
    }
}
