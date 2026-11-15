namespace Asyncomnia.Messaging.Sagas.StateMachine.Behavior;

public sealed class SagaContext<TSagaInstance, TMessage> where TSagaInstance : AsyncomniaSagaInstance
                                                         where TMessage: class, IMessage
{
    public TMessage Message { get; }
    public TSagaInstance State { get; }
    public CancellationToken CancellationToken { get; }
    public IServiceProvider Services { get; }

    private readonly IEventPublisher _eventPublisher;
    private readonly ICommandSender _commandSender;

    internal SagaContext(TMessage message, TSagaInstance state, CancellationToken cancellationToken, IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(serviceProvider);

        Message = message;
        State = state;
        CancellationToken = cancellationToken;
        Services = serviceProvider;

        _eventPublisher = serviceProvider.GetService(typeof(IEventPublisher)) as IEventPublisher
            ?? throw new InvalidOperationException($"Required service '{typeof(IEventPublisher).FullName}' is not registered in the current saga execution scope. Ensure AddAsyncomniaMessaging() has been called.");

        _commandSender = serviceProvider.GetService(typeof(ICommandSender)) as ICommandSender
            ?? throw new InvalidOperationException($"Required service '{typeof(ICommandSender).FullName}' is not registered in the current saga execution scope. Ensure AddAsyncomniaMessaging() has been called.");
    }

    public Task SendAsync<TCommand>(TCommand command, MessageDispatchOptions? options = null) where TCommand : class, ICommand
    {
        ArgumentNullException.ThrowIfNull(command);

        return _commandSender.SendAsync(command, options, CancellationToken);
    }

    public Task PublishAsync<TEvent>(TEvent @event, MessageDispatchOptions? options = null) where TEvent : class, IEvent
    {
        ArgumentNullException.ThrowIfNull(@event);

        return _eventPublisher.PublishAsync(@event, options, CancellationToken);
    }
}