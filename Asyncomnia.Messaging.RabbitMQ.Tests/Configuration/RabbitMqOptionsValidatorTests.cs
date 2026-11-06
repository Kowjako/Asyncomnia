using Asyncomnia.Messaging.RabbitMQ.Configuration;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Configuration;

public sealed class RabbitMqOptionsValidatorTests
{
    private readonly RabbitMqOptionsValidator _validator = new();

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Validate_WhenConnectionStringIsMissing_ReturnsFailure(string? connectionString)
    {
        var options = new RabbitMqOptions
        {
            ConnectionString = connectionString!
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains(nameof(RabbitMqOptions.ConnectionString), result.FailureMessage);
    }

    [Fact]
    public void Validate_WhenConnectionStringIsConfigured_ReturnsSuccess()
    {
        var options = new RabbitMqOptions
        {
            ConnectionString = "host=localhost"
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }
}
