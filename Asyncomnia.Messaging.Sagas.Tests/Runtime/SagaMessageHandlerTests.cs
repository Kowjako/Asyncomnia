using Asyncomnia.Messaging.Sagas.Runtime;
using Asyncomnia.Messaging.Sagas.StateMachine;
using Asyncomnia.Messaging.Sagas.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Sagas.Tests.Runtime;

public sealed class SagaMessageHandlerTests
{
    [Fact]
    public void EventHandlerConstructor_WhenRuntimeIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new SagaEventHandler<EventStateMachine, TestSagaInstance, TestEvent>(null!));

        Assert.Equal("runtime", exception.ParamName);
    }

    [Fact]
    public void CommandHandlerConstructor_WhenRuntimeIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new SagaCommandHandler<CommandStateMachine, TestSagaInstance, TestCommand>(null!));

        Assert.Equal("runtime", exception.ParamName);
    }

    [Fact]
    public async Task EventHandlerHandleAsync_ForwardsMessageMetadataAndCancellationTokenToRuntime()
    {
        var correlationId = Guid.NewGuid();
        var instance = new TestSagaInstance
        {
            CorrelationId = correlationId
        };
        var repository = new StubSagaRepository<TestSagaInstance>();
        repository.EnqueueGetResult(instance);
        var runtime = new SagaRuntime<EventStateMachine, TestSagaInstance>(
            repository,
            new EventStateMachine(),
            new TestServiceProvider());
        var handler = new SagaEventHandler<EventStateMachine, TestSagaInstance, TestEvent>(runtime);
        var message = new TestEvent(correlationId, "event");
        var metadata = CreateMetadata();
        using var cancellationTokenSource = new CancellationTokenSource();

        await handler.HandleAsync(message, metadata, cancellationTokenSource.Token);

        var getCall = Assert.Single(repository.GetCalls);
        Assert.Equal(correlationId, getCall.CorrelationId);
        Assert.Equal(cancellationTokenSource.Token, getCall.CancellationToken);
        Assert.Equal(new[] { "event:event" }, instance.ExecutedActivities);
        Assert.Same(instance, Assert.Single(repository.UpdateCalls).Instance);
    }

    [Fact]
    public async Task CommandHandlerHandleAsync_ForwardsMessageMetadataAndCancellationTokenToRuntime()
    {
        var correlationId = Guid.NewGuid();
        var instance = new TestSagaInstance
        {
            CorrelationId = correlationId
        };
        var repository = new StubSagaRepository<TestSagaInstance>();
        repository.EnqueueGetResult(instance);
        var runtime = new SagaRuntime<CommandStateMachine, TestSagaInstance>(
            repository,
            new CommandStateMachine(),
            new TestServiceProvider());
        var handler = new SagaCommandHandler<CommandStateMachine, TestSagaInstance, TestCommand>(runtime);
        var message = new TestCommand(correlationId, "command");
        var metadata = CreateMetadata();
        using var cancellationTokenSource = new CancellationTokenSource();

        await handler.HandleAsync(message, metadata, cancellationTokenSource.Token);

        var getCall = Assert.Single(repository.GetCalls);
        Assert.Equal(correlationId, getCall.CorrelationId);
        Assert.Equal(cancellationTokenSource.Token, getCall.CancellationToken);
        Assert.Equal(new[] { "command:command" }, instance.ExecutedActivities);
        Assert.Same(instance, Assert.Single(repository.UpdateCalls).Instance);
    }

    private static MessageMetadata CreateMetadata()
    {
        return new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow);
    }

    private sealed class EventStateMachine : AsyncomniaStateMachine<TestSagaInstance>
    {
        public EventStateMachine()
        {
            var testEvent = Event<TestEvent>(configuration =>
                configuration.CorrelateById(message => message.SagaId));

            Initially(
                When(testEvent)
                    .Then(context => context.State.ExecutedActivities.Add($"event:{context.Message.Value}")));
        }
    }

    private sealed class CommandStateMachine : AsyncomniaStateMachine<TestSagaInstance>
    {
        public CommandStateMachine()
        {
            var testCommand = Event<TestCommand>(configuration =>
                configuration.CorrelateById(message => message.SagaId));

            Initially(
                When(testCommand)
                    .Then(context => context.State.ExecutedActivities.Add($"command:{context.Message.Value}")));
        }
    }
}
