namespace Asyncomnia.Messaging.Reliability.Outbox;

public interface IOutboxWriter
{
    Task WriteAsync(OutboxMessage message, CancellationToken token = default);
}