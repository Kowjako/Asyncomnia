namespace Asyncomnia.Messaging.Inbound;

internal static class MessageDispatchDescriptorFactory
{
    public static MessageDispatchDescriptor CreateEvent<TEvent>() where TEvent: class, IEvent
    {
        return new MessageDispatchDescriptor(typeof(TEvent), static (pipe, ctx, token) => 
            pipe.HandleEventAsync<TEvent>(ctx, token));
    }

    public static MessageDispatchDescriptor CreateCommand<TCommand>() where TCommand: class, ICommand
    {
        return new MessageDispatchDescriptor(typeof(TCommand), static (pipe, ctx, token) =>
            pipe.HandleCommandAsync<TCommand>(ctx, token));
    }
}