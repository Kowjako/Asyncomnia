using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Serialization;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using EasyNetQ;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Infrastructure.Amqp.Serialization;

public sealed class RabbitMqMessagePropertiesMapperTests
{
    [Fact]
    public void Map_WithMessageTypeAndMetadata_ReturnsPersistentJsonProperties()
    {
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(
            2026,
            11,
            7,
            12,
            30,
            45,
            TimeSpan.Zero);
        var metadata = new MessageMetadata(messageId, createdAt)
        {
            CorrelationId = correlationId
        };

        var properties = RabbitMqMessagePropertiesMapper.Map(
            typeof(ClientCreatedEvent),
            metadata);

        Assert.Equal(
            RabbitMqMessagePropertiesMapper.JsonContentType,
            properties.ContentType);
        Assert.Equal(
            RabbitMqMessagePropertiesMapper.Utf8ContentEncoding,
            properties.ContentEncoding);
        Assert.Equal(
            "Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles." +
            "ClientCreatedEvent, Asyncomnia.Messaging.RabbitMQ.Tests",
            properties.Type);
        Assert.Equal(messageId.ToString("D"), properties.MessageId);
        Assert.Equal(
            correlationId.ToString("D"),
            properties.CorrelationId);
        Assert.Equal(
            createdAt.ToUnixTimeSeconds(),
            properties.Timestamp.UnixTime);
        Assert.Equal(
            DeliveryModes.Persistent,
            properties.DeliveryMode);
    }

    [Fact]
    public void Map_WithoutCorrelationId_UsesMessageIdAsCorrelationId()
    {
        var messageId = Guid.NewGuid();
        var metadata = new MessageMetadata(
            messageId,
            DateTimeOffset.UtcNow);

        var properties = RabbitMqMessagePropertiesMapper.Map(
            typeof(ClientCreatedEvent),
            metadata);

        Assert.Equal(
            messageId.ToString("D"),
            properties.CorrelationId);
    }

    [Fact]
    public void Map_WhenMessageTypeDoesNotImplementIMessage_Throws()
    {
        var metadata = new MessageMetadata(
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);

        var exception = Assert.Throws<InvalidOperationException>(
            () => RabbitMqMessagePropertiesMapper.Map(
                typeof(string),
                metadata));

        Assert.Contains(nameof(IMessage), exception.Message);
    }

    [Fact]
    public void Map_WhenMessageTypeIsNull_Throws()
    {
        var metadata = new MessageMetadata(
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);

        Assert.Throws<ArgumentNullException>(
            () => RabbitMqMessagePropertiesMapper.Map(
                null!,
                metadata));
    }

    [Fact]
    public void Map_WhenMetadataIsNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => RabbitMqMessagePropertiesMapper.Map(
                typeof(ClientCreatedEvent),
                null!));
    }

    [Fact]
    public void Map_WithValidProperties_ReturnsMessageMetadata()
    {
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(2026, 11, 7, 12, 30, 45, TimeSpan.Zero);
        var properties = new MessageProperties
        {
            MessageId = messageId.ToString("D"),
            CorrelationId = correlationId.ToString("D"),
            Timestamp = createdAt.ToUnixTimeSeconds()
        };

        var metadata = RabbitMqMessagePropertiesMapper.Map(in properties);

        Assert.Equal(messageId, metadata.MessageId);
        Assert.Equal(correlationId, metadata.CorrelationId);
        Assert.Equal(createdAt, metadata.CreatedAt);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Map_WithoutCorrelationId_ReturnsMetadataWithoutCorrelationId(string? correlationId)
    {
        var properties = CreateValidProperties() with
        {
            CorrelationId = correlationId!
        };

        var metadata = RabbitMqMessagePropertiesMapper.Map(in properties);

        Assert.Null(metadata.CorrelationId);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("invalid-message-id")]
    [InlineData("00000000-0000-0000-0000-000000000000")]
    public void Map_WithInvalidMessageId_ThrowsInvalidOperationException(string? messageId)
    {
        var properties = CreateValidProperties() with
        {
            MessageId = messageId!
        };

        var exception = Assert.Throws<InvalidOperationException>(
            () => RabbitMqMessagePropertiesMapper.Map(properties));

        Assert.Contains(nameof(MessageProperties.MessageId), exception.Message);
    }

    [Theory]
    [InlineData("invalid-correlation-id")]
    [InlineData("00000000-0000-0000-0000-000000000000")]
    public void Map_WithInvalidCorrelationId_ThrowsInvalidOperationException(string correlationId)
    {
        var properties = CreateValidProperties() with
        {
            CorrelationId = correlationId
        };

        var exception = Assert.Throws<InvalidOperationException>(
            () => RabbitMqMessagePropertiesMapper.Map(properties));

        Assert.Contains(nameof(MessageProperties.CorrelationId), exception.Message);
    }

    [Theory]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public void Map_WithTimestampOutsideDateTimeOffsetRange_ThrowsInvalidOperationException(long timestamp)
    {
        var properties = CreateValidProperties() with
        {
            Timestamp = timestamp
        };

        var exception = Assert.Throws<InvalidOperationException>(
            () => RabbitMqMessagePropertiesMapper.Map(properties));

        Assert.Contains(nameof(MessageProperties.Timestamp), exception.Message);
        Assert.IsType<ArgumentOutOfRangeException>(exception.InnerException);
    }

    private static MessageProperties CreateValidProperties()
    {
        return new MessageProperties
        {
            MessageId = Guid.NewGuid().ToString("D"),
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
        };
    }
}
