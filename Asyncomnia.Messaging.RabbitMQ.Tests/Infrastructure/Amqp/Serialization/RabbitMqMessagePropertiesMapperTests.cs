using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Serialization;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Infrastructure.Amqp.Serialization;

public sealed class RabbitMqMessagePropertiesMapperTests
{
    [Fact]
    public void MapToBrokerProperties_WithMessageTypeAndMetadata_ReturnsPersistentJsonProperties()
    {
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(2026, 11, 7, 12, 30, 45, TimeSpan.Zero);
        var metadata = new MessageMetadata(messageId, createdAt)
        {
            CorrelationId = correlationId
        };

        var properties = RabbitMqMessagePropertiesMapper.MapToBrokerProperties(
            typeof(ClientCreatedEvent),
            metadata);

        Assert.Equal(RabbitMqMessagePropertiesMapper.JsonContentType, properties.ContentType);
        Assert.Equal(RabbitMqMessagePropertiesMapper.Utf8ContentEncoding, properties.ContentEncoding);
        Assert.Equal(
            "Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles." +
            "ClientCreatedEvent, Asyncomnia.Messaging.RabbitMQ.Tests",
            properties.Type);
        Assert.Equal(messageId.ToString("D"), properties.MessageId);
        Assert.Equal(correlationId.ToString("D"), properties.CorrelationId);
        Assert.Equal(createdAt.ToUnixTimeSeconds(), properties.Timestamp.UnixTime);
        Assert.Equal(DeliveryModes.Persistent, properties.DeliveryMode);
    }

    [Fact]
    public void MapToBrokerProperties_WithoutCorrelationId_UsesMessageIdAsCorrelationId()
    {
        var messageId = Guid.NewGuid();
        var metadata = new MessageMetadata(messageId, DateTimeOffset.UtcNow);

        var properties = RabbitMqMessagePropertiesMapper.MapToBrokerProperties(
            typeof(ClientCreatedEvent),
            metadata);

        Assert.Equal(messageId.ToString("D"), properties.CorrelationId);
    }

    [Fact]
    public void MapToBrokerProperties_WhenMessageTypeDoesNotImplementIMessage_Throws()
    {
        var metadata = new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow);

        var exception = Assert.Throws<InvalidOperationException>(
            () => RabbitMqMessagePropertiesMapper.MapToBrokerProperties(typeof(string), metadata));

        Assert.Contains(nameof(IMessage), exception.Message);
    }

    [Fact]
    public void MapToBrokerProperties_WhenMessageTypeIsNull_Throws()
    {
        var metadata = new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow);

        Assert.Throws<ArgumentNullException>(
            () => RabbitMqMessagePropertiesMapper.MapToBrokerProperties(null!, metadata));
    }

    [Fact]
    public void MapToBrokerProperties_WhenMetadataIsNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => RabbitMqMessagePropertiesMapper.MapToBrokerProperties(
                typeof(ClientCreatedEvent),
                null!));
    }

    [Fact]
    public void MapToAsyncomniaProperties_WithValidProperties_ReturnsMessageMetadata()
    {
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(2026, 11, 7, 12, 30, 45, TimeSpan.Zero);
        var properties = new BasicProperties
        {
            MessageId = messageId.ToString("D"),
            CorrelationId = correlationId.ToString("D"),
            Timestamp = new AmqpTimestamp(createdAt.ToUnixTimeSeconds())
        };

        var metadata = RabbitMqMessagePropertiesMapper.MapToAsyncomniaProperties(properties);

        Assert.Equal(messageId, metadata.MessageId);
        Assert.Equal(correlationId, metadata.CorrelationId);
        Assert.Equal(createdAt, metadata.CreatedAt);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("invalid-message-id")]
    [InlineData("00000000-0000-0000-0000-000000000000")]
    public void MapToAsyncomniaProperties_WithInvalidMessageId_Throws(string? messageId)
    {
        var properties = CreateValidProperties();
        properties.MessageId = messageId!;

        var exception = Assert.Throws<InvalidOperationException>(
            () => RabbitMqMessagePropertiesMapper.MapToAsyncomniaProperties(properties));

        Assert.Contains(nameof(BasicProperties.MessageId), exception.Message);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("invalid-correlation-id")]
    [InlineData("00000000-0000-0000-0000-000000000000")]
    public void MapToAsyncomniaProperties_WithInvalidCorrelationId_Throws(string? correlationId)
    {
        var properties = CreateValidProperties();
        properties.CorrelationId = correlationId!;

        var exception = Assert.Throws<InvalidOperationException>(
            () => RabbitMqMessagePropertiesMapper.MapToAsyncomniaProperties(properties));

        Assert.Contains(nameof(BasicProperties.CorrelationId), exception.Message);
    }

    [Theory]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public void MapToAsyncomniaProperties_WithTimestampOutsideDateTimeOffsetRange_Throws(long timestamp)
    {
        var properties = CreateValidProperties();
        properties.Timestamp = new AmqpTimestamp(timestamp);

        var exception = Assert.Throws<InvalidOperationException>(
            () => RabbitMqMessagePropertiesMapper.MapToAsyncomniaProperties(properties));

        Assert.Contains(nameof(BasicProperties.Timestamp), exception.Message);
        Assert.IsType<ArgumentOutOfRangeException>(exception.InnerException);
    }

    [Fact]
    public void MapToAsyncomniaProperties_WhenPropertiesAreNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => RabbitMqMessagePropertiesMapper.MapToAsyncomniaProperties(null!));
    }

    private static BasicProperties CreateValidProperties()
    {
        return new BasicProperties
        {
            MessageId = Guid.NewGuid().ToString("D"),
            CorrelationId = Guid.NewGuid().ToString("D"),
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
        };
    }
}
