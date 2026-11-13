namespace Asyncomnia.Messaging.Reliability.DelayedPublishing;

public interface IDelayedMessageWriter
{
    Task WriteAsync(DelayedMessage message, CancellationToken token = default);

    Task<bool> CancelAsync(Guid scheduleId, CancellationToken token = default);
}
