using Asyncomnia.Messaging.RabbitMQ.Configuration;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Configuration;

public sealed class RabbitMqConsumerOptionsValidatorTests
{
    private readonly RabbitMqConsumerOptionsValidator _validator = new();

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Validate_WhenEndpointNameIsMissing_ReturnsFailure(string? endpointName)
    {
        var options = new RabbitMqConsumerOptions
        {
            EndpointName = endpointName!
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains(nameof(RabbitMqConsumerOptions.EndpointName), result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenEndpointNameIsConfigured_ReturnsSuccess()
    {
        var options = new RabbitMqConsumerOptions
        {
            EndpointName = "billing-service"
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_WhenDefaultConsumerCountIsNotPositive_ReturnsFailure(
        int consumerCount)
    {
        var options = CreateValidOptions();
        options.DefaultConsumerCount = consumerCount;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains(nameof(options.DefaultConsumerCount), result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenDefaultPrefetchCountIsZero_ReturnsFailure()
    {
        var options = CreateValidOptions();
        options.DefaultPrefetchCount = 0;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains(nameof(options.DefaultPrefetchCount), result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenDefaultDispatcherConcurrencyIsZero_ReturnsFailure()
    {
        var options = CreateValidOptions();
        options.DefaultDispatcherConcurrency = 0;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains(nameof(options.DefaultDispatcherConcurrency), result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_WhenSubscriptionConsumerCountIsNotPositive_ReturnsFailure(
        int consumerCount)
    {
        var options = CreateValidOptions();
        options.ConfigureSubscription(
            "commands/long-work-job",
            subscription => subscription.ConsumerCount = consumerCount);

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("commands/long-work-job", result.FailureMessage);
        Assert.Contains(nameof(RabbitMqSubscriptionConsumerOptions.ConsumerCount), result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenSubscriptionPrefetchCountIsZero_ReturnsFailure()
    {
        var options = CreateValidOptions();
        options.ConfigureSubscription(
            "commands/long-work-job",
            subscription => subscription.PrefetchCount = 0);

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("commands/long-work-job", result.FailureMessage);
        Assert.Contains(nameof(RabbitMqSubscriptionConsumerOptions.PrefetchCount), result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenSubscriptionDispatcherConcurrencyIsZero_ReturnsFailure()
    {
        var options = CreateValidOptions();
        options.ConfigureSubscription(
            "commands/long-work-job",
            subscription => subscription.DispatcherConcurrency = 0);

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("commands/long-work-job", result.FailureMessage);
        Assert.Contains(nameof(RabbitMqSubscriptionConsumerOptions.DispatcherConcurrency), result.FailureMessage);
    }

    [Fact]
    public void Validate_WithValidDefaultsAndSubscriptionOverrides_ReturnsSuccess()
    {
        var options = CreateValidOptions();
        options.ConfigureSubscription(
            "commands/long-work-job",
            subscription =>
            {
                subscription.ConsumerCount = 2;
                subscription.PrefetchCount = 1;
                subscription.DispatcherConcurrency = 1;
            });

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }

    private static RabbitMqConsumerOptions CreateValidOptions()
    {
        return new RabbitMqConsumerOptions
        {
            EndpointName = "test-service"
        };
    }
}
