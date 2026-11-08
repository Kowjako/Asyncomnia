using Asyncomnia.Messaging.Retry;

namespace Asyncomnia.Messaging.Tests.Retry;

public sealed class InMemoryRetryOptionsValidatorTests
{
    private readonly InMemoryRetryOptionsValidator _validator = new();

    [Fact]
    public void Validate_WhenStrategyIsNotConfigured_ReturnsFailure()
    {
        var options = new InMemoryRetryOptions();

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("Immediately()", result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenImmediateStrategyIsConfigured_ReturnsSuccess()
    {
        var options = new InMemoryRetryOptions();
        options.Immediately(2);

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void Validate_WhenIntervalStrategyIsConfigured_ReturnsSuccess()
    {
        var options = new InMemoryRetryOptions();
        options.Intervals(TimeSpan.FromMilliseconds(100));

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }
}
