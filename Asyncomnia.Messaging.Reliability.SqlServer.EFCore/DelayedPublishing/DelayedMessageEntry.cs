using Asyncomnia.Messaging.Reliability.DelayedPublishing;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DelayedPublishing;

internal sealed class DelayedMessageEntry
{
    public Guid ScheduleId { get; private set; }
    public Guid MessageId { get; private set; }
    public string ContractType { get; private set; } = null!;
    public string MessageJson { get; private set; } = null!;
    public Guid? CorrelationId { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }
    public DateTimeOffset DeliverAt { get; private set; }

    public DelayedMessageEntry(DelayedMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);

        ScheduleId = message.ScheduleId;
        MessageId = message.MessageId;
        ContractType = message.ContractType;
        MessageJson = message.MessageJson;
        CorrelationId = message.CorrelationId;
        CreatedAt = message.CreatedAt;
        DeliverAt = message.DeliverAt;
    }

    private DelayedMessageEntry() { }
}