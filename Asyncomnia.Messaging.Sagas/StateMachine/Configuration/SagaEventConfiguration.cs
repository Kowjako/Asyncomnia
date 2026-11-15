namespace Asyncomnia.Messaging.Sagas.StateMachine.Configuration;

public sealed class SagaEventConfiguration<TMessage> where TMessage: class, IMessage
{
    private Func<TMessage, Guid>? _correlationIdSelector;

    public SagaEventConfiguration<TMessage> CorrelateById(Func<TMessage, Guid> correlationIdSelector)
    {
        ArgumentNullException.ThrowIfNull(correlationIdSelector);
        _correlationIdSelector = correlationIdSelector;
        return this;
    }

    internal SagaEvent<TMessage> Build()
    {
        if (_correlationIdSelector == null)
        {
            throw new InvalidOperationException(
                $"Event '{typeof(TMessage).FullName}' must configure CorrelateById before it can be registered.");
        }

        return new SagaEvent<TMessage>(_correlationIdSelector);
    }
}
