using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Outbox;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.Outbox;

public sealed class OutboxEntryTests
{
    [Fact]
    public void Constructor_WithOutboxMessage_CopiesMessageData()
    {
        var createdAt = new DateTimeOffset(2026, 11, 6, 10, 30, 0, TimeSpan.Zero);
        var message = new OutboxMessage(
            Guid.NewGuid(),
            "Tests.OrderCreated, Tests",
            "{\"orderId\":42}",
            Guid.NewGuid(),
            createdAt);

        var entry = new OutboxEntry(message);

        Assert.Equal(message.MessageId, entry.MessageId);
        Assert.Equal(message.ContractType, entry.ContractType);
        Assert.Equal(message.MessageJson, entry.MessageJson);
        Assert.Equal(message.CorrelationId, entry.CorrelationId);
        Assert.Equal(message.CreatedAt, entry.CreatedAt);
        Assert.Equal(message.CreatedAt, entry.AvailableAt);
    }
}
