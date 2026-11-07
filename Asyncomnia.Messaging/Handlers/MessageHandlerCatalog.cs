namespace Asyncomnia.Messaging.Handlers;

public interface IMessageHandlerCatalog
{
    IReadOnlyCollection<Type> EventTypes { get; }

    IReadOnlyCollection<Type> CommandTypes { get; }
}

internal sealed class MessageHandlerCatalog : IMessageHandlerCatalog
{
    public IReadOnlyCollection<Type> EventTypes { get; }

    public IReadOnlyCollection<Type> CommandTypes { get; }

    public MessageHandlerCatalog(IEnumerable<MessageHandlerRegistration> registrations)
    {
        var messageTypes = registrations.Select(x => x.MessageType).Distinct().ToArray();
        EventTypes = messageTypes.Where(typeof(IEvent).IsAssignableFrom).ToArray();
        CommandTypes = messageTypes.Where(typeof(ICommand).IsAssignableFrom).ToArray();
    }
}