namespace Asyncomnia.Messaging.Sagas.StateMachine.Configuration;

public interface ISagaEvent
{
    Type Type { get; }
    Guid GetCorrelationId(object message, MessageMetadata metadata);
}

public sealed class SagaEvent<TMessage> : ISagaEvent where TMessage: class, IMessage
{
    public Type Type => typeof(TMessage);

    private readonly Func<TMessage, MessageMetadata, Guid> _correlationIdSelector;

    internal SagaEvent(Func<TMessage, MessageMetadata, Guid> correlationIdSelector)
    {
        _correlationIdSelector = correlationIdSelector ?? throw new ArgumentNullException(nameof(correlationIdSelector));
    }

    public Guid GetCorrelationId(object message, MessageMetadata metadata)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(metadata);

        if (message is not TMessage typedMessage)
        {
            throw new ArgumentException($"Expected message type '{typeof(TMessage).FullName}', but received '{message.GetType().FullName}'.", nameof(message));
        }

        var correlationId = _correlationIdSelector(typedMessage, metadata);

        if (correlationId == Guid.Empty)
        {
            throw new InvalidOperationException($"Correlation selector for event '{typeof(TMessage).FullName}' returned Guid.Empty.");
        }

        return correlationId;
    }
}