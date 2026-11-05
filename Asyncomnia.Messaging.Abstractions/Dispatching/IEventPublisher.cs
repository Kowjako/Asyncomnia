namespace Asyncomnia.Messaging;

public interface IEventPublisher
{
    Task PublishAsync<TEvent>(TEvent @event, MessageDispatchOptions? options = null, CancellationToken token = default)
        where TEvent : class, IEvent;
}