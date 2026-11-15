using Asyncomnia.Messaging.Sagas.StateMachine.Behavior;
using Asyncomnia.Messaging.Sagas.StateMachine.Configuration;
using Asyncomnia.Messaging.Sagas.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Sagas.Tests.StateMachine.Behavior;

public sealed class SagaBehaviorConfigurationTests
{
    [Fact]
    public void Constructor_WhenSagaEventIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new SagaBehaviorConfiguration<TestSagaInstance, TestEvent>(
                null!,
                [State.Initial, State.Final]));

        Assert.Equal("sagaEvent", exception.ParamName);
    }

    [Fact]
    public void Constructor_WhenRegisteredStatesIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new SagaBehaviorConfiguration<TestSagaInstance, TestEvent>(
                CreateSagaEvent(),
                null!));

        Assert.Equal("registeredStates", exception.ParamName);
    }

    [Fact]
    public void MessageType_ReturnsConfiguredMessageType()
    {
        var behavior = CreateBehavior();

        Assert.Equal(typeof(TestEvent), behavior.MessageType);
    }

    [Fact]
    public void Then_ReturnsSameBehavior()
    {
        var behavior = CreateBehavior();

        var result = behavior.Then(_ => { });

        Assert.Same(behavior, result);
    }

    [Fact]
    public async Task Then_ExecutesActivitiesInRegistrationOrder()
    {
        var behavior = CreateBehavior()
            .Then(context => context.State.ExecutedActivities.Add("first"))
            .Then(context => context.State.ExecutedActivities.Add("second"));
        var state = new TestSagaInstance();

        await ExecuteAsync(behavior, state);

        Assert.Equal(["first", "second"], state.ExecutedActivities);
    }

    [Fact]
    public void Then_WhenActionIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = Assert.Throws<ArgumentNullException>(() => behavior.Then(null!));

        Assert.Equal("action", exception.ParamName);
    }

    [Fact]
    public async Task ThenAsync_ExecutesAndPreservesActivityOrder()
    {
        var behavior = CreateBehavior()
            .Then(context => context.State.ExecutedActivities.Add("sync"))
            .ThenAsync(async context =>
            {
                await Task.Yield();
                context.State.ExecutedActivities.Add("async");
            })
            .Then(context => context.State.ExecutedActivities.Add("last"));
        var state = new TestSagaInstance();

        await ExecuteAsync(behavior, state);

        Assert.Equal(["sync", "async", "last"], state.ExecutedActivities);
    }

    [Fact]
    public void ThenAsync_WhenActionIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = Assert.Throws<ArgumentNullException>(() => behavior.ThenAsync(null!));

        Assert.Equal("action", exception.ParamName);
    }

    [Fact]
    public async Task If_WhenPredicateIsTrue_ExecutesConfiguredBranch()
    {
        var behavior = CreateBehavior()
            .If(
                context => context.State.Condition,
                branch => branch.Then(context => context.State.ExecutedActivities.Add("if")));
        var state = new TestSagaInstance { Condition = true };

        await ExecuteAsync(behavior, state);

        Assert.Equal(["if"], state.ExecutedActivities);
    }

    [Fact]
    public async Task If_WhenPredicateIsFalse_SkipsConfiguredBranch()
    {
        var behavior = CreateBehavior()
            .If(
                context => context.State.Condition,
                branch => branch.Then(context => context.State.ExecutedActivities.Add("if")));
        var state = new TestSagaInstance { Condition = false };

        await ExecuteAsync(behavior, state);

        Assert.Empty(state.ExecutedActivities);
    }

    [Fact]
    public void If_WhenPredicateIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = Assert.Throws<ArgumentNullException>(
            () => behavior.If(null!, _ => { }));

        Assert.Equal("predicate", exception.ParamName);
    }

    [Fact]
    public void If_WhenBranchConfigurationIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = Assert.Throws<ArgumentNullException>(
            () => behavior.If(_ => true, null!));

        Assert.Equal("action", exception.ParamName);
    }

    [Fact]
    public async Task IfAsync_UsesAsynchronousPredicateAndBranch()
    {
        var behavior = CreateBehavior()
            .IfAsync(
                async context =>
                {
                    await Task.Yield();
                    return context.State.Condition;
                },
                branch => branch.ThenAsync(async context =>
                {
                    await Task.Yield();
                    context.State.ExecutedActivities.Add("if-async");
                }));
        var state = new TestSagaInstance { Condition = true };

        await ExecuteAsync(behavior, state);

        Assert.Equal(["if-async"], state.ExecutedActivities);
    }

    [Fact]
    public async Task IfAsync_WhenPredicateIsFalse_SkipsConfiguredBranch()
    {
        var behavior = CreateBehavior()
            .IfAsync(
                _ => Task.FromResult(false),
                branch => branch.Then(context => context.State.ExecutedActivities.Add("if")));
        var state = new TestSagaInstance();

        await ExecuteAsync(behavior, state);

        Assert.Empty(state.ExecutedActivities);
    }

    [Fact]
    public void IfAsync_WhenPredicateIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = Assert.Throws<ArgumentNullException>(
            () => behavior.IfAsync(null!, _ => { }));

        Assert.Equal("predicate", exception.ParamName);
    }

    [Fact]
    public void IfAsync_WhenBranchConfigurationIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = Assert.Throws<ArgumentNullException>(
            () => behavior.IfAsync(_ => Task.FromResult(true), null!));

        Assert.Equal("action", exception.ParamName);
    }

    [Theory]
    [InlineData(true, "if")]
    [InlineData(false, "else")]
    public async Task IfElse_ExecutesOnlySelectedBranch(bool condition, string expectedActivity)
    {
        var behavior = CreateBehavior()
            .IfElse(
                context => context.State.Condition,
                ifBranch => ifBranch.Then(context => context.State.ExecutedActivities.Add("if")),
                elseBranch => elseBranch.Then(context => context.State.ExecutedActivities.Add("else")));
        var state = new TestSagaInstance { Condition = condition };

        await ExecuteAsync(behavior, state);

        Assert.Equal([expectedActivity], state.ExecutedActivities);
    }

    [Fact]
    public void IfElse_WhenPredicateIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = Assert.Throws<ArgumentNullException>(
            () => behavior.IfElse(null!, _ => { }, _ => { }));

        Assert.Equal("predicate", exception.ParamName);
    }

    [Fact]
    public void IfElse_WhenIfBranchIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = Assert.Throws<ArgumentNullException>(
            () => behavior.IfElse(_ => true, null!, _ => { }));

        Assert.Equal("ifAction", exception.ParamName);
    }

    [Fact]
    public void IfElse_WhenElseBranchIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = Assert.Throws<ArgumentNullException>(
            () => behavior.IfElse(_ => true, _ => { }, null!));

        Assert.Equal("elseAction", exception.ParamName);
    }

    [Theory]
    [InlineData(true, "if-async")]
    [InlineData(false, "else-async")]
    public async Task IfElseAsync_ExecutesOnlySelectedBranch(
        bool condition,
        string expectedActivity)
    {
        var behavior = CreateBehavior()
            .IfElseAsync(
                _ => Task.FromResult(condition),
                ifBranch => ifBranch.Then(context => context.State.ExecutedActivities.Add("if-async")),
                elseBranch => elseBranch.Then(context => context.State.ExecutedActivities.Add("else-async")));
        var state = new TestSagaInstance();

        await ExecuteAsync(behavior, state);

        Assert.Equal([expectedActivity], state.ExecutedActivities);
    }

    [Fact]
    public void IfElseAsync_WhenPredicateIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = Assert.Throws<ArgumentNullException>(
            () => behavior.IfElseAsync(null!, _ => { }, _ => { }));

        Assert.Equal("predicate", exception.ParamName);
    }

    [Fact]
    public void IfElseAsync_WhenIfBranchIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = Assert.Throws<ArgumentNullException>(
            () => behavior.IfElseAsync(_ => Task.FromResult(true), null!, _ => { }));

        Assert.Equal("ifAction", exception.ParamName);
    }

    [Fact]
    public void IfElseAsync_WhenElseBranchIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = Assert.Throws<ArgumentNullException>(
            () => behavior.IfElseAsync(_ => Task.FromResult(true), _ => { }, null!));

        Assert.Equal("elseAction", exception.ParamName);
    }

    [Fact]
    public async Task TransitionTo_ChangesCurrentState()
    {
        var submitted = new State("Submitted");
        var behavior = CreateBehavior(submitted).TransitionTo(submitted);
        var state = new TestSagaInstance();

        await ExecuteAsync(behavior, state);

        Assert.Equal(submitted.Name, state.CurrentState);
    }

    [Fact]
    public void TransitionTo_WhenStateIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = Assert.Throws<ArgumentNullException>(
            () => behavior.TransitionTo(null!));

        Assert.Equal("state", exception.ParamName);
    }

    [Fact]
    public void TransitionTo_WhenStateIsNotRegistered_ThrowsDescriptiveException()
    {
        var behavior = CreateBehavior();

        var exception = Assert.Throws<InvalidOperationException>(
            () => behavior.TransitionTo(new State("Unknown")));

        Assert.Contains("Unknown", exception.Message);
        Assert.Contains("Register states before configuring transitions", exception.Message);
    }

    [Fact]
    public void TransitionTo_WhenStateWasRegisteredAfterBehaviorCreation_ThrowsInvalidOperationException()
    {
        var machine = new TestStateMachine();
        var sagaEvent = machine.ConfigureEvent<TestEvent>(
            configuration => configuration.CorrelateById(message => message.SagaId));
        var behavior = machine.ConfigureWhen(sagaEvent);
        var state = machine.ConfigureState("RegisteredTooLate");

        var exception = Assert.Throws<InvalidOperationException>(
            () => behavior.TransitionTo(state));

        Assert.Contains("RegisteredTooLate", exception.Message);
    }

    [Fact]
    public async Task MultipleTransitions_LastExecutedTransitionWins()
    {
        var submitted = new State("Submitted");
        var accepted = new State("Accepted");
        var behavior = CreateBehavior(submitted, accepted)
            .TransitionTo(submitted)
            .TransitionTo(accepted);
        var state = new TestSagaInstance();

        await ExecuteAsync(behavior, state);

        Assert.Equal(accepted.Name, state.CurrentState);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TransitionAfterIfElse_OverridesBranchTransition(bool condition)
    {
        var accepted = new State("Accepted");
        var rejected = new State("Rejected");
        var completed = new State("Completed");
        var behavior = CreateBehavior(accepted, rejected, completed)
            .IfElse(
                _ => condition,
                branch => branch.TransitionTo(accepted),
                branch => branch.TransitionTo(rejected))
            .TransitionTo(completed);
        var state = new TestSagaInstance();

        await ExecuteAsync(behavior, state);

        Assert.Equal(completed.Name, state.CurrentState);
    }

    [Fact]
    public async Task Finalize_ChangesCurrentStateToFinal()
    {
        var behavior = CreateBehavior().Finalize();
        var state = new TestSagaInstance();

        await ExecuteAsync(behavior, state);

        Assert.Equal(State.Final.Name, state.CurrentState);
    }

    [Fact]
    public async Task ExecuteAsync_WithoutActivities_DoesNotChangeState()
    {
        var behavior = CreateBehavior();
        var state = new TestSagaInstance { CurrentState = "Existing" };

        await ExecuteAsync(behavior, state);

        Assert.Equal("Existing", state.CurrentState);
        Assert.Empty(state.ExecutedActivities);
    }

    [Fact]
    public async Task ExecuteAsync_ProvidesMessageStateServicesAndTokenToActivity()
    {
        var behavior = CreateBehavior();
        var message = new TestEvent(Guid.NewGuid());
        var state = new TestSagaInstance();
        var serviceProvider = new TestServiceProvider();
        var dependency = new ScopedDependency();
        serviceProvider.Add(dependency);
        using var cancellationTokenSource = new CancellationTokenSource();
        SagaContext<TestSagaInstance, TestEvent>? capturedContext = null;
        behavior.Then(context => capturedContext = context);

        await behavior.ExecuteAsync(
            state,
            message,
            serviceProvider,
            cancellationTokenSource.Token);

        Assert.NotNull(capturedContext);
        Assert.Same(message, capturedContext.Message);
        Assert.Same(state, capturedContext.State);
        Assert.Same(serviceProvider, capturedContext.Services);
        Assert.Same(dependency, capturedContext.Services.GetService(typeof(ScopedDependency)));
        Assert.Equal(cancellationTokenSource.Token, capturedContext.CancellationToken);
    }

    [Fact]
    public async Task ExecuteAsync_WhenStateIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = await Assert.ThrowsAsync<ArgumentNullException>(
            () => behavior.ExecuteAsync(
                null!,
                new TestEvent(Guid.NewGuid()),
                new TestServiceProvider(),
                default).AsTask());

        Assert.Equal("body", exception.ParamName);
    }

    [Fact]
    public async Task ExecuteAsync_WhenMessageIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = await Assert.ThrowsAsync<ArgumentNullException>(
            () => behavior.ExecuteAsync(
                new TestSagaInstance(),
                null!,
                new TestServiceProvider(),
                default).AsTask());

        Assert.Equal("message", exception.ParamName);
    }

    [Fact]
    public async Task ExecuteAsync_WhenMessageHasDifferentType_ThrowsArgumentException()
    {
        var behavior = CreateBehavior();

        var exception = await Assert.ThrowsAsync<ArgumentException>(
            () => behavior.ExecuteAsync(
                new TestSagaInstance(),
                new SecondTestEvent(Guid.NewGuid()),
                new TestServiceProvider(),
                default).AsTask());

        Assert.Equal("message", exception.ParamName);
        Assert.Contains(typeof(TestEvent).FullName!, exception.Message);
        Assert.Contains(typeof(SecondTestEvent).FullName!, exception.Message);
    }

    [Fact]
    public async Task ExecuteAsync_WhenServiceProviderIsNull_ThrowsArgumentNullException()
    {
        var behavior = CreateBehavior();

        var exception = await Assert.ThrowsAsync<ArgumentNullException>(
            () => behavior.ExecuteAsync(
                new TestSagaInstance(),
                new TestEvent(Guid.NewGuid()),
                null!,
                default).AsTask());

        Assert.Equal("serviceProvider", exception.ParamName);
    }

    [Fact]
    public async Task ExecuteAsync_WhenTokenIsAlreadyCancelled_DoesNotExecuteActivities()
    {
        var behavior = CreateBehavior()
            .Then(context => context.State.ExecutedActivities.Add("not-executed"));
        var state = new TestSagaInstance();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            behavior.ExecuteAsync(
                state,
                new TestEvent(Guid.NewGuid()),
                new TestServiceProvider(),
                cancellationTokenSource.Token).AsTask());

        Assert.Empty(state.ExecutedActivities);
    }

    [Fact]
    public async Task ExecuteAsync_WhenActivityCancelsToken_DoesNotExecuteFollowingActivity()
    {
        using var cancellationTokenSource = new CancellationTokenSource();
        var behavior = CreateBehavior()
            .Then(context =>
            {
                context.State.ExecutedActivities.Add("first");
                cancellationTokenSource.Cancel();
            })
            .Then(context => context.State.ExecutedActivities.Add("not-executed"));
        var state = new TestSagaInstance();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            behavior.ExecuteAsync(
                state,
                new TestEvent(Guid.NewGuid()),
                new TestServiceProvider(),
                cancellationTokenSource.Token).AsTask());

        Assert.Equal(["first"], state.ExecutedActivities);
    }

    [Fact]
    public async Task ExecuteAsync_WhenNestedActivityCancelsToken_SkipsRemainingNestedActivities()
    {
        using var cancellationTokenSource = new CancellationTokenSource();
        var behavior = CreateBehavior()
            .If(
                _ => true,
                branch => branch
                    .Then(context =>
                    {
                        context.State.ExecutedActivities.Add("nested-first");
                        cancellationTokenSource.Cancel();
                    })
                    .Then(context => context.State.ExecutedActivities.Add("not-executed")));
        var state = new TestSagaInstance();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            behavior.ExecuteAsync(
                state,
                new TestEvent(Guid.NewGuid()),
                new TestServiceProvider(),
                cancellationTokenSource.Token).AsTask());

        Assert.Equal(["nested-first"], state.ExecutedActivities);
    }

    [Fact]
    public async Task ExecuteAsync_WhenActivityThrows_PropagatesExceptionAndStopsPipeline()
    {
        var expectedException = new TestException();
        var behavior = CreateBehavior()
            .Then(_ => throw expectedException)
            .Then(context => context.State.ExecutedActivities.Add("not-executed"));
        var state = new TestSagaInstance();

        var exception = await Assert.ThrowsAsync<TestException>(
            () => ExecuteAsync(behavior, state));

        Assert.Same(expectedException, exception);
        Assert.Empty(state.ExecutedActivities);
    }

    private static SagaBehaviorConfiguration<TestSagaInstance, TestEvent> CreateBehavior(
        params State[] additionalStates)
    {
        State[] registeredStates = [State.Initial, State.Final, .. additionalStates];

        return new SagaBehaviorConfiguration<TestSagaInstance, TestEvent>(
            CreateSagaEvent(),
            registeredStates);
    }

    private static SagaEvent<TestEvent> CreateSagaEvent()
    {
        return new SagaEvent<TestEvent>(message => message.SagaId);
    }

    private static Task ExecuteAsync(
        SagaBehaviorConfiguration<TestSagaInstance, TestEvent> behavior,
        TestSagaInstance state)
    {
        return behavior.ExecuteAsync(
            state,
            new TestEvent(Guid.NewGuid()),
            new TestServiceProvider(),
            default).AsTask();
    }

    private sealed class TestException : Exception;
}
