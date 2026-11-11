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

    [Theory]
    [InlineData("host=localhost")]
    [InlineData("localhost")]
    [InlineData("http://localhost")]
    public void Validate_WhenConnectionStringIsNotAnAmqpUri_ReturnsFailure(
        string connectionString)
    {
        var options = new RabbitMqOptions
        {
            ConnectionString = connectionString
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains(nameof(RabbitMqOptions.ConnectionString), result.FailureMessage);
    }

    [Theory]
    [InlineData("amqp://guest:guest@localhost:5672/")]
    [InlineData("amqps://rabbitmq.example.com/")]
    public void Validate_WhenConnectionStringIsAnAmqpUri_ReturnsSuccess(
        string connectionString)
    {
        var options = new RabbitMqOptions
        {
            ConnectionString = connectionString
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_WhenPublisherChannelCountIsNotPositive_ReturnsFailure(
        int publisherChannelCount)
    {
        var options = new RabbitMqOptions
        {
            ConnectionString =
                "amqp://guest:guest@localhost:5672/",
            PublisherChannelCount = publisherChannelCount
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains(
            nameof(RabbitMqOptions.PublisherChannelCount),
            result.FailureMessage);
    }
}
