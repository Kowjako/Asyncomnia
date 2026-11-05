using Asyncomnia.Messaging.Reliability.Outbox;

namespace Asyncomnia.Messaging.Reliability.Tests.Outbox;

public sealed class OutboxMessageTests
{
    [Fact]
    public void Constructor_WithValidValues_PreservesValues()
    {
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var createdAt = DateTimeOffset.UtcNow;

        var message = new OutboxMessage(
            messageId,
            "Example.Message, Example.Contracts",
            "{\"value\":\"test\"}",
            correlationId,
            createdAt);

        Assert.Equal(messageId, message.MessageId);
        Assert.Equal("Example.Message, Example.Contracts", message.ContractType);
        Assert.Equal("{\"value\":\"test\"}", message.MessageJson);
        Assert.Equal(correlationId, message.CorrelationId);
        Assert.Equal(createdAt, message.CreatedAt);
    }

    [Fact]
    public void Constructor_WhenMessageIdIsEmpty_Throws()
    {
        Assert.Throws<ArgumentException>(
            () => new OutboxMessage(
                Guid.Empty,
                "Example.Message, Example.Contracts",
                "{}",
                null,
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Constructor_WhenContractTypeIsEmpty_Throws()
    {
        Assert.Throws<ArgumentException>(
            () => new OutboxMessage(
                Guid.NewGuid(),
                " ",
                "{}",
                null,
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Constructor_WhenMessageJsonIsEmpty_Throws()
    {
        Assert.Throws<ArgumentException>(
            () => new OutboxMessage(
                Guid.NewGuid(),
                "Example.Message, Example.Contracts",
                " ",
                null,
                DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Constructor_WhenCorrelationIdIsEmpty_Throws()
    {
        Assert.Throws<ArgumentException>(
            () => new OutboxMessage(
                Guid.NewGuid(),
                "Example.Message, Example.Contracts",
                "{}",
                Guid.Empty,
                DateTimeOffset.UtcNow));
    }
}
