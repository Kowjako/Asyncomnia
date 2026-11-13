using Asyncomnia.Messaging.Reliability.Inbox;

namespace Asyncomnia.Messaging.Reliability.Tests.Inbox;

public sealed class InboxMessageTests
{
    [Fact]
    public void Constructor_WithValidValues_SetsProperties()
    {
        var messageId = Guid.NewGuid();
        var receivedAt = new DateTimeOffset(2026, 11, 12, 10, 30, 0, TimeSpan.Zero);

        var message = new InboxMessage(
            messageId,
            "events/clients/subscription/billing-service",
            "Tests.ClientCreatedEvent, Tests",
            receivedAt);

        Assert.Equal(messageId, message.MessageId);
        Assert.Equal("events/clients/subscription/billing-service", message.ConsumerId);
        Assert.Equal("Tests.ClientCreatedEvent, Tests", message.ContractType);
        Assert.Equal(receivedAt, message.ReceivedAt);
    }

    [Fact]
    public void Constructor_WithEmptyMessageId_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            new InboxMessage(
                Guid.Empty,
                "consumer",
                "contract",
                DateTimeOffset.UtcNow));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_WithInvalidConsumerId_ThrowsArgumentException(
        string? consumerId)
    {
        Assert.ThrowsAny<ArgumentException>(() =>
            new InboxMessage(
                Guid.NewGuid(),
                consumerId!,
                "contract",
                DateTimeOffset.UtcNow));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_WithInvalidContractType_ThrowsArgumentException(
        string? contractType)
    {
        Assert.ThrowsAny<ArgumentException>(() =>
            new InboxMessage(
                Guid.NewGuid(),
                "consumer",
                contractType!,
                DateTimeOffset.UtcNow));
    }
}
