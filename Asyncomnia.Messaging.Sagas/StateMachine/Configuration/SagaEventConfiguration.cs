namespace Asyncomnia.Messaging.Sagas.StateMachine.Configuration;

public sealed class SagaEventConfiguration<TMessage> where TMessage: class, IMessage
{
    private Func<TMessage, MessageMetadata, Guid>? _correlationIdSelector;

    public SagaEventConfiguration<TMessage> CorrelateById(Func<TMessage, Guid> correlationIdSelector)
    {
        ArgumentNullException.ThrowIfNull(correlationIdSelector);
        _correlationIdSelector = (message, _) => correlationIdSelector(message);
        return this;
    }

    public SagaEventConfiguration<TMessage> CorrelateByMessageCorrelationId()
    {
        _correlationIdSelector = (_, metadata) => metadata.CorrelationId
            ?? throw new InvalidOperationException($"Message '{typeof(TMessage).FullName}' does not contain a correlation ID.");

        return this;
    }

    internal SagaEvent<TMessage> Build()
    {
        if (_correlationIdSelector == null)
        {
            throw new InvalidOperationException($"Event '{typeof(TMessage).FullName}' must configure a correlation strategy using {nameof(CorrelateById)} or {nameof(CorrelateByMessageCorrelationId)} before it can be registered.");
        }

        return new SagaEvent<TMessage>(_correlationIdSelector);
    }
}
