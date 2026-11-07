using Asyncomnia.Messaging.RabbitMQ.Consumer;
using EasyNetQ;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer;

public sealed class RabbitMqMessageMetadataMapperTests
{
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

        var metadata = RabbitMqMessageMetadataMapper.Map(in properties);

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

        var metadata = RabbitMqMessageMetadataMapper.Map(in properties);

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
            () => RabbitMqMessageMetadataMapper.Map(properties));

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
            () => RabbitMqMessageMetadataMapper.Map(properties));

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
            () => RabbitMqMessageMetadataMapper.Map(properties));

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
