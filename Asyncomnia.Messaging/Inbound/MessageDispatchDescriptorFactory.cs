namespace Asyncomnia.Messaging.Inbound;

internal static class MessageDispatchDescriptorFactory
{
    public static MessageDispatchDescriptor CreateEvent<TEvent>() where TEvent: class, IEvent
    {
        return new MessageDispatchDescriptor(typeof(TEvent), static (pipe, msg, meta, token) => 
            pipe.HandleEventAsync(new MessageEnvelope<TEvent>((TEvent)msg, meta), token));
    }

    public static MessageDispatchDescriptor CreateCommand<TCommand>() where TCommand: class, ICommand
    {
        return new MessageDispatchDescriptor(typeof(TCommand), static (pipe, msg, meta, token) =>
            pipe.HandleCommandAsync(new MessageEnvelope<TCommand>((TCommand)msg, meta), token));
    }
}