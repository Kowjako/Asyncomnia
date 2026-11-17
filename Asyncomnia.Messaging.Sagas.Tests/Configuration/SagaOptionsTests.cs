using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Sagas.Configuration;
using Asyncomnia.Messaging.Sagas.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Sagas.Tests.Configuration;

public sealed class SagaOptionsTests
{
    [Fact]
    public void ConfigureSagas_WhenBuilderIsNull_ThrowsArgumentNullException()
    {
        AsyncomniaMessagingBuilder builder = null!;

        var exception = Assert.Throws<ArgumentNullException>(() =>
            builder.ConfigureSagas(_ => { }));

        Assert.Equal("builder", exception.ParamName);
    }

    [Fact]
    public void ConfigureSagas_WhenConfigureIsNull_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        var exception = Assert.Throws<ArgumentNullException>(() =>
            builder.ConfigureSagas(null!));

        Assert.Equal("configure", exception.ParamName);
    }

    [Fact]
    public void ConfigureSagas_ReturnsSameBuilderAndUsesConfiguredOptions()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        var returnedBuilder = builder.ConfigureSagas(options =>
        {
            options.CleanupEnabled = false;
            options.FinalizedSagaRetention = TimeSpan.FromDays(14);
            options.CleanupBatchSize = 250;
            options.CleanupInterval = TimeSpan.FromMinutes(2);
        });
        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<SagaOptions>>().Value;

        Assert.Same(builder, returnedBuilder);
        Assert.False(options.CleanupEnabled);
        Assert.Equal(TimeSpan.FromDays(14), options.FinalizedSagaRetention);
        Assert.Equal(250, options.CleanupBatchSize);
        Assert.Equal(TimeSpan.FromMinutes(2), options.CleanupInterval);
    }

    [Fact]
    public void SagaOptions_UsesSafeCleanupDefaults()
    {
        var options = new SagaOptions();

        Assert.True(options.CleanupEnabled);
        Assert.Equal(TimeSpan.FromDays(30), options.FinalizedSagaRetention);
        Assert.Equal(1000, options.CleanupBatchSize);
        Assert.Equal(TimeSpan.FromMinutes(5), options.CleanupInterval);
    }
}
