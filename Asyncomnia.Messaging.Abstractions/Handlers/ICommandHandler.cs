namespace Asyncomnia.Messaging.Handlers;

public interface ICommandHandler<in TCommand> : IMessageHandler<TCommand> where TCommand : class, ICommand;