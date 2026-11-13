using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DelayedPublishing;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.DelayedPublishing;

public sealed class DelayedMessageEntryTests
{
    [Fact]
    public void Constructor_WithDelayedMessage_CopiesMessageData()
    {
        var createdAt = new DateTimeOffset(2026, 11, 15, 10, 0, 0, TimeSpan.Zero);
        var message = new DelayedMessage(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "Tests.ReportRequested, Tests",
            "{\"reportId\":42}",
            Guid.NewGuid(),
            createdAt,
            createdAt.AddHours(2));

        var entry = new DelayedMessageEntry(message);

        Assert.Equal(message.ScheduleId, entry.ScheduleId);
        Assert.Equal(message.MessageId, entry.MessageId);
        Assert.Equal(message.ContractType, entry.ContractType);
        Assert.Equal(message.MessageJson, entry.MessageJson);
        Assert.Equal(message.CorrelationId, entry.CorrelationId);
        Assert.Equal(message.CreatedAt, entry.CreatedAt);
        Assert.Equal(message.DeliverAt, entry.DeliverAt);
    }

    [Fact]
    public void Constructor_WhenMessageIsNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new DelayedMessageEntry(null!));
    }
}
