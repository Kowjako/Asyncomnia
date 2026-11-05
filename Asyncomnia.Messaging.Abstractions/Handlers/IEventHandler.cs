namespace Asyncomnia.Messaging.Handlers;

public interface IEventHandler<in TEvent> : IMessageHandler<TEvent> where TEvent : class, IEvent;