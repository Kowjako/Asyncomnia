namespace Asyncomnia.Messaging;

public interface IMessageScheduler
{
    Task<Guid> ScheduleAsync<TMessage>(TMessage message, DateTimeOffset deliverAt, MessageDispatchOptions? options = null, CancellationToken token = default)
        where TMessage : class, IMessage;

    Task<Guid> ScheduleAsync<TMessage>(TMessage message, TimeSpan delay, MessageDispatchOptions? options = null, CancellationToken token = default)
        where TMessage : class, IMessage;

    Task<bool> CancelAsync(Guid scheduleId, CancellationToken token = default);
}