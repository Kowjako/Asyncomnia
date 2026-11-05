using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;

namespace Asyncomnia.Messaging.Reliability.Tests.Outbox.Storage;

public sealed class ClaimedOutboxMessageTests
{
    [Fact]
    public void Constructor_WithValidValues_PreservesValues()
    {
        var message = CreateOutboxMessage();
        var leaseId = Guid.NewGuid();
        var leaseExpiresAt = DateTimeOffset.UtcNow.AddMinutes(1);

        var claimedMessage = new ClaimedOutboxMessage(
            message,
            leaseId,
            2,
            leaseExpiresAt);

        Assert.Same(message, claimedMessage.Message);
        Assert.Equal(leaseId, claimedMessage.LeaseId);
        Assert.Equal(2, claimedMessage.AttemptNumber);
        Assert.Equal(leaseExpiresAt, claimedMessage.LeaseExpiresAt);
    }

    [Fact]
    public void Constructor_WhenMessageIsNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => new ClaimedOutboxMessage(
                null!,
                Guid.NewGuid(),
                1,
                DateTimeOffset.UtcNow.AddMinutes(1)));
    }

    [Fact]
    public void Constructor_WhenLeaseIdIsEmpty_Throws()
    {
        Assert.Throws<ArgumentException>(
            () => new ClaimedOutboxMessage(
                CreateOutboxMessage(),
                Guid.Empty,
                1,
                DateTimeOffset.UtcNow.AddMinutes(1)));
    }

    [Fact]
    public void Constructor_WhenAttemptNumberIsNotPositive_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new ClaimedOutboxMessage(
                CreateOutboxMessage(),
                Guid.NewGuid(),
                0,
                DateTimeOffset.UtcNow.AddMinutes(1)));
    }

    private static OutboxMessage CreateOutboxMessage()
    {
        return new OutboxMessage(
            Guid.NewGuid(),
            "Example.Message, Example.Contracts",
            "{}",
            null,
            DateTimeOffset.UtcNow);
    }
}
