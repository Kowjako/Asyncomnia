using Asyncomnia.Messaging.Reliability.Outbox;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Outbox;

internal sealed class OutboxEntry
{
    public Guid MessageId { get; private set; }
    public string ContractType { get; private set; } = null!;
    public string MessageJson { get; private set; } = null!;
    public Guid? CorrelationId { get; private set; }
    public DateTimeOffset AvailableAt { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }

    public OutboxEntry(OutboxMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);

        MessageId = message.MessageId;
        ContractType = message.ContractType;
        MessageJson = message.MessageJson;
        CorrelationId = message.CorrelationId;
        CreatedAt = message.CreatedAt;
        AvailableAt = message.CreatedAt;
    }

    private OutboxEntry() { }
}
