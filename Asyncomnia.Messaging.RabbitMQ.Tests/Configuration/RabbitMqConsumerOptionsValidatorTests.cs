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
}
