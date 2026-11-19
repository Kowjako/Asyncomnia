using Asyncomnia.Messaging.Sagas.StateMachine.Configuration;

namespace Asyncomnia.Messaging.Sagas.StateMachine;

public abstract class AsyncomniaSagaInstance
{
    /// <summary>
    /// Describes current Saga state, by default each saga starts with 'Initial' state
    /// </summary>
    public string CurrentState { get; internal set; } = State.Initial.Name;

    /// <summary>
    /// Unique identifier for Saga instance
    /// </summary>
    public Guid CorrelationId { get; internal set; }
}