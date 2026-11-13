using Asyncomnia.Messaging.Reliability.DelayedPublishing;

namespace Asyncomnia.Messaging.Reliability.Tests.DelayedPublishing;

public sealed class DelayedMessageTests
{
    [Fact]
    public void Constructor_WithValidValues_PreservesValues()
    {
        var scheduleId = Guid.NewGuid();
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(2026, 11, 15, 10, 0, 0, TimeSpan.Zero);
        var deliverAt = createdAt.AddHours(2);

        var message = new DelayedMessage(
            scheduleId,
            messageId,
            "Tests.ReportRequested, Tests",
            "{\"reportId\":42}",
            correlationId,
            createdAt,
            deliverAt);

        Assert.Equal(scheduleId, message.ScheduleId);
        Assert.Equal(messageId, message.MessageId);
        Assert.Equal("Tests.ReportRequested, Tests", message.ContractType);
        Assert.Equal("{\"reportId\":42}", message.MessageJson);
        Assert.Equal(correlationId, message.CorrelationId);
        Assert.Equal(createdAt, message.CreatedAt);
        Assert.Equal(deliverAt, message.DeliverAt);
    }

    [Fact]
    public void Constructor_WhenScheduleIdIsEmpty_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => CreateMessage(scheduleId: Guid.Empty));
    }

    [Fact]
    public void Constructor_WhenMessageIdIsEmpty_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => CreateMessage(messageId: Guid.Empty));
    }

    [Fact]
    public void Constructor_WhenContractTypeIsNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => CreateMessage(contractType: null!));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Constructor_WhenContractTypeIsInvalid_ThrowsArgumentException(
        string contractType)
    {
        Assert.Throws<ArgumentException>(() => CreateMessage(contractType: contractType));
    }

    [Fact]
    public void Constructor_WhenMessageJsonIsNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => CreateMessage(messageJson: null!));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Constructor_WhenMessageJsonIsInvalid_ThrowsArgumentException(
        string messageJson)
    {
        Assert.Throws<ArgumentException>(() => CreateMessage(messageJson: messageJson));
    }

    [Fact]
    public void Constructor_WhenCorrelationIdIsEmpty_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => CreateMessage(correlationId: Guid.Empty));
    }

    private static DelayedMessage CreateMessage(
        Guid? scheduleId = null,
        Guid? messageId = null,
        string contractType = "Tests.ReportRequested, Tests",
        string messageJson = "{}",
        Guid? correlationId = null)
    {
        return new DelayedMessage(
            scheduleId ?? Guid.NewGuid(),
            messageId ?? Guid.NewGuid(),
            contractType,
            messageJson,
            correlationId,
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow.AddMinutes(5));
    }
}
