namespace Asyncomnia.Messaging.Sagas.StateMachine.Configuration;

public interface ISagaEvent
{
    Type Type { get; }
    Guid GetCorrelationId(object message);
}

public sealed class SagaEvent<TMessage> : ISagaEvent where TMessage: class, IMessage
{
    public Type Type => typeof(TMessage);

    private Func<TMessage, Guid> _correlateByIdSelector = null!;

    internal SagaEvent(Func<TMessage, Guid> correlateByIdSelector)
    {
        _correlateByIdSelector = correlateByIdSelector;
    }

    public Guid GetCorrelationId(object message)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (message is not TMessage typedMessage)
        {
            throw new ArgumentException($"Expected message of type {typeof(TMessage).FullName}', but received '{message.GetType().FullName}'",  nameof(message));
        }

        var correlationId = _correlateByIdSelector(typedMessage);

        if (correlationId == Guid.Empty)
        {
            throw new InvalidOperationException($"Event '{Type}' returned an empty CorrelationId.");
        }

        return correlationId;
    }
}