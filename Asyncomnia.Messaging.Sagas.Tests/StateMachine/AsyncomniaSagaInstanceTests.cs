using Asyncomnia.Messaging.Sagas.StateMachine.Configuration;
using Asyncomnia.Messaging.Sagas.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Sagas.Tests.StateMachine;

public sealed class AsyncomniaSagaInstanceTests
{
    [Fact]
    public void Constructor_UsesInitialStateAndEmptyCorrelationId()
    {
        var instance = new TestSagaInstance();

        Assert.Equal(State.Initial.Name, instance.CurrentState);
        Assert.Equal(Guid.Empty, instance.CorrelationId);
    }

    [Fact]
    public void RuntimeProperties_CanBeSetBySagaRuntimeAssembly()
    {
        var correlationId = Guid.NewGuid();
        var instance = new TestSagaInstance
        {
            CorrelationId = correlationId,
            CurrentState = "Submitted"
        };

        Assert.Equal(correlationId, instance.CorrelationId);
        Assert.Equal("Submitted", instance.CurrentState);
    }
}
