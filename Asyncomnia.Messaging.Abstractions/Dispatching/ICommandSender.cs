namespace Asyncomnia.Messaging;

public interface ICommandSender
{
    Task SendAsync<TCommand>(TCommand command, MessageDispatchOptions? options = null, CancellationToken token = default)
        where TCommand : class, ICommand;
}