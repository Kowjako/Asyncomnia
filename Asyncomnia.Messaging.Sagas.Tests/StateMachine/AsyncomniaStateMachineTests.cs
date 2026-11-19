using Asyncomnia.Messaging.Sagas.StateMachine.Behavior;
using Asyncomnia.Messaging.Sagas.StateMachine.Configuration;
using Asyncomnia.Messaging.Sagas.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Sagas.Tests.StateMachine;

public sealed class AsyncomniaStateMachineTests
{
    [Fact]
    public void Constructor_RegistersInitialAndFinalStates()
    {
        var machine = new TestStateMachine();

        Assert.Equal(2, machine.States.Count);
        Assert.Contains(State.Initial, machine.States);
        Assert.Contains(State.Final, machine.States);
    }

    [Fact]
    public void State_RegistersCustomState()
    {
        var machine = new TestStateMachine();

        var submitted = machine.ConfigureState("Submitted");

        Assert.Contains(submitted, machine.States);
    }

    [Fact]
    public void State_WhenNameIsAlreadyRegistered_ThrowsInvalidOperationException()
    {
        var machine = new TestStateMachine();
        machine.ConfigureState("Submitted");

        var exception = Assert.Throws<InvalidOperationException>(
            () => machine.ConfigureState("Submitted"));

        Assert.Contains(typeof(TestStateMachine).FullName!, exception.Message);
        Assert.Contains("Submitted", exception.Message);
    }

    [Fact]
    public void State_WhenBuiltInNameIsUsed_ThrowsInvalidOperationException()
    {
        var machine = new TestStateMachine();

        var exception = Assert.Throws<InvalidOperationException>(
            () => machine.ConfigureState(State.Initial.Name));

        Assert.Contains(State.Initial.Name, exception.Message);
    }

    [Fact]
    public void Event_RegistersEventAndMakesItAvailableToRuntime()
    {
        var machine = new TestStateMachine();
        var sagaEvent = machine.ConfigureEvent<TestEvent>(
            eventConfiguration => eventConfiguration.CorrelateById(message => message.SagaId));

        var found = machine.TryGetEvent(typeof(TestEvent), out var result);

        Assert.True(found);
        Assert.Same(sagaEvent, result);
    }

    [Fact]
    public void Event_WhenConfigurationIsNull_ThrowsArgumentNullException()
    {
        var machine = new TestStateMachine();

        var exception = Assert.Throws<ArgumentNullException>(
            () => machine.ConfigureEvent<TestEvent>(null!));

        Assert.Equal("configuration", exception.ParamName);
    }

    [Fact]
    public void Event_WhenMessageTypeIsAlreadyRegistered_ThrowsInvalidOperationException()
    {
        var machine = new TestStateMachine();
        machine.ConfigureEvent<TestEvent>(
            eventConfiguration => eventConfiguration.CorrelateById(message => message.SagaId));

        var exception = Assert.Throws<InvalidOperationException>(() =>
            machine.ConfigureEvent<TestEvent>(
                eventConfiguration => eventConfiguration.CorrelateById(message => message.SagaId)));

        Assert.Contains(typeof(TestStateMachine).FullName!, exception.Message);
        Assert.Contains(typeof(TestEvent).FullName!, exception.Message);
    }

    [Fact]
    public void When_WithRegisteredEvent_CreatesBehaviorForMessageType()
    {
        var machine = new TestStateMachine();
        var sagaEvent = RegisterTestEvent(machine);

        var behavior = machine.ConfigureWhen(sagaEvent);

        Assert.Equal(typeof(TestEvent), behavior.MessageType);
    }

    [Fact]
    public void When_WhenEventIsNull_ThrowsArgumentNullException()
    {
        var machine = new TestStateMachine();

        var exception = Assert.Throws<ArgumentNullException>(
            () => machine.ConfigureWhen<TestEvent>(null!));

        Assert.Contains("event", exception.ParamName!);
    }

    [Fact]
    public void When_WhenEventTypeIsNotRegistered_ThrowsInvalidOperationException()
    {
        var machine = new TestStateMachine();
        var otherMachine = new TestStateMachine();
        var foreignEvent = RegisterTestEvent(otherMachine);

        var exception = Assert.Throws<InvalidOperationException>(
            () => machine.ConfigureWhen(foreignEvent));

        Assert.Contains(typeof(TestEvent).FullName!, exception.Message);
        Assert.Contains("not registered", exception.Message);
    }

    [Fact]
    public void When_WhenEventBelongsToAnotherMachine_ThrowsInvalidOperationException()
    {
        var machine = new TestStateMachine();
        var otherMachine = new TestStateMachine();
        RegisterTestEvent(machine);
        var foreignEvent = RegisterTestEvent(otherMachine);

        var exception = Assert.Throws<InvalidOperationException>(
            () => machine.ConfigureWhen(foreignEvent));

        Assert.Contains(typeof(TestEvent).FullName!, exception.Message);
        Assert.Contains("does not belong", exception.Message);
    }

    [Fact]
    public void Initially_RegistersBehaviorForInitialState()
    {
        var machine = new TestStateMachine();
        var behavior = CreateTestEventBehavior(machine);

        machine.ConfigureInitially(behavior);

        Assert.True(machine.TryGetBehavior(
            State.Initial.Name,
            typeof(TestEvent),
            out var registeredBehavior));
        Assert.Same(behavior, registeredBehavior);
    }

    [Fact]
    public void During_RegistersBehaviorForCustomState()
    {
        var machine = new TestStateMachine();
        var submitted = machine.ConfigureState("Submitted");
        var behavior = CreateTestEventBehavior(machine);

        machine.ConfigureDuring(submitted, behavior);

        Assert.True(machine.TryGetBehavior(
            submitted.Name,
            typeof(TestEvent),
            out var registeredBehavior));
        Assert.Same(behavior, registeredBehavior);
    }

    [Fact]
    public void During_WhenStateIsNull_ThrowsArgumentNullException()
    {
        var machine = new TestStateMachine();

        var exception = Assert.Throws<ArgumentNullException>(
            () => machine.ConfigureDuring(null!));

        Assert.Equal("state", exception.ParamName);
    }

    [Fact]
    public void During_WhenStateIsNotRegistered_ThrowsInvalidOperationException()
    {
        var machine = new TestStateMachine();

        var exception = Assert.Throws<InvalidOperationException>(
            () => machine.ConfigureDuring(new State("Unknown")));

        Assert.Contains("Unknown", exception.Message);
        Assert.Contains("Register the state", exception.Message);
    }

    [Fact]
    public void RegisterBehaviors_WhenCollectionIsNull_ThrowsArgumentNullException()
    {
        var machine = new TestStateMachine();

        var exception = Assert.Throws<ArgumentNullException>(
            () => machine.ConfigureInitially(null!));

        Assert.Equal("definitions", exception.ParamName);
    }

    [Fact]
    public void RegisterBehaviors_WhenCollectionContainsNull_ThrowsArgumentException()
    {
        var machine = new TestStateMachine();

        var exception = Assert.Throws<ArgumentException>(() =>
            machine.ConfigureInitially((SagaBehaviorConfiguration<TestSagaInstance>)null!));

        Assert.Equal("definitions", exception.ParamName);
        Assert.Contains(State.Initial.Name, exception.Message);
    }

    [Fact]
    public void RegisterBehaviors_WhenStateAndMessageAreDuplicated_ThrowsInvalidOperationException()
    {
        var machine = new TestStateMachine();
        var sagaEvent = RegisterTestEvent(machine);
        var first = machine.ConfigureWhen(sagaEvent);
        var second = machine.ConfigureWhen(sagaEvent);

        var exception = Assert.Throws<InvalidOperationException>(
            () => machine.ConfigureInitially(first, second));

        Assert.Contains(State.Initial.Name, exception.Message);
        Assert.Contains(typeof(TestEvent).FullName!, exception.Message);
    }

    [Fact]
    public void SameMessageBehavior_CanBeRegisteredForDifferentStates()
    {
        var machine = new TestStateMachine();
        var submitted = machine.ConfigureState("Submitted");
        var sagaEvent = RegisterTestEvent(machine);
        var initialBehavior = machine.ConfigureWhen(sagaEvent);
        var submittedBehavior = machine.ConfigureWhen(sagaEvent);

        machine.ConfigureInitially(initialBehavior);
        machine.ConfigureDuring(submitted, submittedBehavior);

        Assert.True(machine.TryGetBehavior(State.Initial.Name, typeof(TestEvent), out _));
        Assert.True(machine.TryGetBehavior(submitted.Name, typeof(TestEvent), out _));
    }

    [Fact]
    public void TryGetBehavior_WhenBehaviorDoesNotExist_ReturnsFalseAndNull()
    {
        var machine = new TestStateMachine();

        var found = machine.TryGetBehavior(State.Initial.Name, typeof(TestEvent), out var behavior);

        Assert.False(found);
        Assert.Null(behavior);
    }

    [Fact]
    public void TryGetBehavior_WhenStateNameIsInvalid_ThrowsArgumentException()
    {
        var machine = new TestStateMachine();

        var exception = Assert.Throws<ArgumentException>(
            () => machine.TryGetBehavior(" ", typeof(TestEvent), out _));

        Assert.Equal("currentState", exception.ParamName);
    }

    [Fact]
    public void TryGetBehavior_WhenMessageTypeIsNull_ThrowsArgumentNullException()
    {
        var machine = new TestStateMachine();

        var exception = Assert.Throws<ArgumentNullException>(
            () => machine.TryGetBehavior(State.Initial.Name, null!, out _));

        Assert.Equal("messageType", exception.ParamName);
    }

    [Fact]
    public void TryGetEvent_WhenEventDoesNotExist_ReturnsFalseAndNull()
    {
        var machine = new TestStateMachine();

        var found = machine.TryGetEvent(typeof(TestEvent), out var sagaEvent);

        Assert.False(found);
        Assert.Null(sagaEvent);
    }

    [Fact]
    public void TryGetEvent_WhenMessageTypeIsNull_ThrowsArgumentNullException()
    {
        var machine = new TestStateMachine();

        var exception = Assert.Throws<ArgumentNullException>(
            () => machine.TryGetEvent(null!, out _));

        Assert.Equal("messageType", exception.ParamName);
    }

    private static SagaEvent<TestEvent> RegisterTestEvent(TestStateMachine machine)
    {
        return machine.ConfigureEvent<TestEvent>(
            eventConfiguration => eventConfiguration.CorrelateById(message => message.SagaId));
    }

    private static SagaBehaviorConfiguration<TestSagaInstance, TestEvent> CreateTestEventBehavior(
        TestStateMachine machine)
    {
        return machine.ConfigureWhen(RegisterTestEvent(machine));
    }
}
