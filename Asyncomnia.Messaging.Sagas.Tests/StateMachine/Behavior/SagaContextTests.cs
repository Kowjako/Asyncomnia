using Asyncomnia.Messaging.Sagas.StateMachine.Behavior;
using Asyncomnia.Messaging.Sagas.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Sagas.Tests.StateMachine.Behavior;

public sealed class SagaContextTests
{
    [Fact]
    public void Constructor_SetsExecutionPropertiesAndServices()
    {
        var message = new TestEvent(Guid.NewGuid());
        var state = new TestSagaInstance();
        using var cancellationTokenSource = new CancellationTokenSource();
        var serviceProvider = new TestServiceProvider();
        var scopedDependency = new ScopedDependency();
        serviceProvider.Add(scopedDependency);

        var context = new SagaContext<TestSagaInstance, TestEvent>(
            message,
            state,
            cancellationTokenSource.Token,
            serviceProvider);

        Assert.Same(message, context.Message);
        Assert.Same(state, context.State);
        Assert.Equal(cancellationTokenSource.Token, context.CancellationToken);
        Assert.Same(serviceProvider, context.Services);
        Assert.Same(scopedDependency, context.Services.GetService(typeof(ScopedDependency)));
    }

    [Fact]
    public void Constructor_WithoutMessageScheduler_DoesNotThrow()
    {
        var serviceProvider = new TestServiceProvider(includeMessageScheduler: false);

        var context = CreateContext(serviceProvider);

        Assert.NotNull(context);
    }

    [Fact]
    public void Constructor_WhenMessageIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new SagaContext<TestSagaInstance, TestEvent>(
                null!,
                new TestSagaInstance(),
                default,
                new TestServiceProvider()));

        Assert.Equal("message", exception.ParamName);
    }

    [Fact]
    public void Constructor_WhenStateIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new SagaContext<TestSagaInstance, TestEvent>(
                new TestEvent(Guid.NewGuid()),
                null!,
                default,
                new TestServiceProvider()));

        Assert.Equal("state", exception.ParamName);
    }

    [Fact]
    public void Constructor_WhenServiceProviderIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new SagaContext<TestSagaInstance, TestEvent>(
                new TestEvent(Guid.NewGuid()),
                new TestSagaInstance(),
                default,
                null!));

        Assert.Equal("serviceProvider", exception.ParamName);
    }

    [Fact]
    public void Constructor_WithoutEventPublisher_ThrowsDescriptiveException()
    {
        var serviceProvider = new TestServiceProvider(includeEventPublisher: false);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            new SagaContext<TestSagaInstance, TestEvent>(
                new TestEvent(Guid.NewGuid()),
                new TestSagaInstance(),
                default,
                serviceProvider));

        Assert.Contains(typeof(IEventPublisher).FullName!, exception.Message);
        Assert.Contains("AddAsyncomniaMessaging", exception.Message);
    }

    [Fact]
    public void Constructor_WithoutCommandSender_ThrowsDescriptiveException()
    {
        var serviceProvider = new TestServiceProvider(includeCommandSender: false);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            new SagaContext<TestSagaInstance, TestEvent>(
                new TestEvent(Guid.NewGuid()),
                new TestSagaInstance(),
                default,
                serviceProvider));

        Assert.Contains(typeof(ICommandSender).FullName!, exception.Message);
        Assert.Contains("AddAsyncomniaMessaging", exception.Message);
    }

    [Fact]
    public async Task SendAsync_WithoutOptions_UsesSagaCorrelationId()
    {
        var sagaCorrelationId = Guid.NewGuid();
        var serviceProvider = new TestServiceProvider();
        var context = CreateContext(serviceProvider, sagaCorrelationId: sagaCorrelationId);
        var command = new TestCommand(Guid.NewGuid());

        await context.SendAsync(command);

        Assert.Same(command, serviceProvider.CommandSender.SentCommand);
        Assert.Equal(sagaCorrelationId, serviceProvider.CommandSender.Options?.CorrelationId);
        Assert.Equal(1, serviceProvider.CommandSender.Calls);
    }

    [Fact]
    public async Task SendAsync_WithCorrelationId_ForwardsOptionsAndCancellationToken()
    {
        var serviceProvider = new TestServiceProvider();
        using var cancellationTokenSource = new CancellationTokenSource();
        var context = CreateContext(serviceProvider, cancellationTokenSource.Token);
        var command = new TestCommand(Guid.NewGuid());
        var options = new MessageDispatchOptions { CorrelationId = Guid.NewGuid() };

        await context.SendAsync(command, options);

        Assert.Same(command, serviceProvider.CommandSender.SentCommand);
        Assert.Same(options, serviceProvider.CommandSender.Options);
        Assert.Equal(cancellationTokenSource.Token, serviceProvider.CommandSender.CancellationToken);
        Assert.Equal(1, serviceProvider.CommandSender.Calls);
    }

    [Fact]
    public async Task PublishAsync_WithoutCorrelationIdInOptions_UsesSagaCorrelationId()
    {
        var sagaCorrelationId = Guid.NewGuid();
        var serviceProvider = new TestServiceProvider();
        var context = CreateContext(serviceProvider, sagaCorrelationId: sagaCorrelationId);
        var @event = new SecondTestEvent(Guid.NewGuid());

        await context.PublishAsync(@event, new MessageDispatchOptions());

        Assert.Same(@event, serviceProvider.EventPublisher.PublishedEvent);
        Assert.Equal(sagaCorrelationId, serviceProvider.EventPublisher.Options?.CorrelationId);
        Assert.Equal(1, serviceProvider.EventPublisher.Calls);
    }

    [Fact]
    public async Task PublishAsync_WithCorrelationId_ForwardsOptionsAndCancellationToken()
    {
        var serviceProvider = new TestServiceProvider();
        using var cancellationTokenSource = new CancellationTokenSource();
        var context = CreateContext(serviceProvider, cancellationTokenSource.Token);
        var @event = new SecondTestEvent(Guid.NewGuid());
        var options = new MessageDispatchOptions { CorrelationId = Guid.NewGuid() };

        await context.PublishAsync(@event, options);

        Assert.Same(@event, serviceProvider.EventPublisher.PublishedEvent);
        Assert.Same(options, serviceProvider.EventPublisher.Options);
        Assert.Equal(cancellationTokenSource.Token, serviceProvider.EventPublisher.CancellationToken);
        Assert.Equal(1, serviceProvider.EventPublisher.Calls);
    }

    [Fact]
    public async Task SendAsync_WhenCommandIsNull_ThrowsWithoutCallingSender()
    {
        var serviceProvider = new TestServiceProvider();
        var context = CreateContext(serviceProvider);

        var exception = await Assert.ThrowsAsync<ArgumentNullException>(
            () => context.SendAsync<TestCommand>(null!));

        Assert.Equal("command", exception.ParamName);
        Assert.Equal(0, serviceProvider.CommandSender.Calls);
    }

    [Fact]
    public async Task PublishAsync_WhenEventIsNull_ThrowsWithoutCallingPublisher()
    {
        var serviceProvider = new TestServiceProvider();
        var context = CreateContext(serviceProvider);

        var exception = await Assert.ThrowsAsync<ArgumentNullException>(
            () => context.PublishAsync<SecondTestEvent>(null!));

        Assert.Contains("event", exception.ParamName!);
        Assert.Equal(0, serviceProvider.EventPublisher.Calls);
    }

    [Fact]
    public async Task Dispatch_WhenSagaCorrelationIdIsEmpty_ThrowsWithoutCallingDependency()
    {
        var serviceProvider = new TestServiceProvider();
        var context = CreateContext(serviceProvider, sagaCorrelationId: Guid.Empty);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => context.PublishAsync(new SecondTestEvent(Guid.NewGuid())));

        Assert.Contains(typeof(TestSagaInstance).FullName!, exception.Message);
        Assert.Contains("must have a correlation ID", exception.Message);
        Assert.Equal(0, serviceProvider.EventPublisher.Calls);
    }

    [Fact]
    public async Task ScheduleAsync_WithDelay_ForwardsMessageOptionsAndCancellationToken()
    {
        var sagaCorrelationId = Guid.NewGuid();
        var serviceProvider = new TestServiceProvider(includeMessageScheduler: true);
        using var cancellationTokenSource = new CancellationTokenSource();
        var context = CreateContext(
            serviceProvider,
            cancellationTokenSource.Token,
            sagaCorrelationId);
        var message = new ScheduledTimeout();
        var delay = TimeSpan.FromMinutes(5);

        var scheduleId = await context.ScheduleAsync(message, delay);

        Assert.Equal(serviceProvider.MessageScheduler.ScheduleId, scheduleId);
        Assert.Same(message, serviceProvider.MessageScheduler.ScheduledMessage);
        Assert.Equal(delay, serviceProvider.MessageScheduler.Delay);
        Assert.Null(serviceProvider.MessageScheduler.DeliverAt);
        Assert.Equal(sagaCorrelationId, serviceProvider.MessageScheduler.Options?.CorrelationId);
        Assert.Equal(
            cancellationTokenSource.Token,
            serviceProvider.MessageScheduler.CancellationToken);
        Assert.Equal(1, serviceProvider.MessageScheduler.ScheduleCalls);
    }

    [Fact]
    public async Task ScheduleAsync_WithAbsoluteDate_ForwardsExplicitOptions()
    {
        var serviceProvider = new TestServiceProvider(includeMessageScheduler: true);
        var context = CreateContext(serviceProvider);
        var message = new ScheduledTimeout();
        var deliverAt = new DateTimeOffset(2026, 11, 15, 18, 0, 0, TimeSpan.Zero);
        var options = new MessageDispatchOptions { CorrelationId = Guid.NewGuid() };

        var scheduleId = await context.ScheduleAsync(message, deliverAt, options);

        Assert.Equal(serviceProvider.MessageScheduler.ScheduleId, scheduleId);
        Assert.Same(message, serviceProvider.MessageScheduler.ScheduledMessage);
        Assert.Equal(deliverAt, serviceProvider.MessageScheduler.DeliverAt);
        Assert.Null(serviceProvider.MessageScheduler.Delay);
        Assert.Same(options, serviceProvider.MessageScheduler.Options);
        Assert.Equal(1, serviceProvider.MessageScheduler.ScheduleCalls);
    }

    [Fact]
    public async Task ScheduleAsync_WithDelayAndNullMessage_ThrowsWithoutCallingScheduler()
    {
        var serviceProvider = new TestServiceProvider(includeMessageScheduler: true);
        var context = CreateContext(serviceProvider);

        var exception = await Assert.ThrowsAsync<ArgumentNullException>(() =>
            context.ScheduleAsync<ScheduledTimeout>(null!, TimeSpan.FromMinutes(1)));

        Assert.Equal("message", exception.ParamName);
        Assert.Equal(0, serviceProvider.MessageScheduler.ScheduleCalls);
    }

    [Fact]
    public async Task ScheduleAsync_WithAbsoluteDateAndNullMessage_ThrowsWithoutCallingScheduler()
    {
        var serviceProvider = new TestServiceProvider(includeMessageScheduler: true);
        var context = CreateContext(serviceProvider);

        var exception = await Assert.ThrowsAsync<ArgumentNullException>(() =>
            context.ScheduleAsync<ScheduledTimeout>(null!, DateTimeOffset.UtcNow));

        Assert.Equal("message", exception.ParamName);
        Assert.Equal(0, serviceProvider.MessageScheduler.ScheduleCalls);
    }

    [Fact]
    public async Task ScheduleAsync_WithoutRegisteredScheduler_ThrowsDescriptiveException()
    {
        var serviceProvider = new TestServiceProvider(includeMessageScheduler: false);
        var context = CreateContext(serviceProvider);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            context.ScheduleAsync(new ScheduledTimeout(), TimeSpan.FromMinutes(1)));

        Assert.Contains(typeof(IMessageScheduler).FullName!, exception.Message);
        Assert.Contains("UseDelayedPublishing", exception.Message);
    }

    [Fact]
    public async Task CancelScheduledMessageAsync_ForwardsScheduleIdAndCancellationToken()
    {
        var serviceProvider = new TestServiceProvider(includeMessageScheduler: true);
        serviceProvider.MessageScheduler.CancelResult = false;
        using var cancellationTokenSource = new CancellationTokenSource();
        var context = CreateContext(serviceProvider, cancellationTokenSource.Token);
        var scheduleId = Guid.NewGuid();

        var result = await context.CancelScheduledMessageAsync(scheduleId);

        Assert.False(result);
        Assert.Equal(scheduleId, serviceProvider.MessageScheduler.CancelledScheduleId);
        Assert.Equal(
            cancellationTokenSource.Token,
            serviceProvider.MessageScheduler.CancellationTokenAtCancellation);
        Assert.Equal(1, serviceProvider.MessageScheduler.CancelCalls);
    }

    [Fact]
    public async Task CancelScheduledMessageAsync_WhenScheduleIdIsEmpty_ThrowsWithoutCallingScheduler()
    {
        var serviceProvider = new TestServiceProvider(includeMessageScheduler: true);
        var context = CreateContext(serviceProvider);

        var exception = await Assert.ThrowsAsync<ArgumentException>(
            () => context.CancelScheduledMessageAsync(Guid.Empty));

        Assert.Equal("scheduleId", exception.ParamName);
        Assert.Equal(0, serviceProvider.MessageScheduler.CancelCalls);
    }

    [Fact]
    public async Task CancelScheduledMessageAsync_WithoutRegisteredScheduler_ThrowsDescriptiveException()
    {
        var serviceProvider = new TestServiceProvider(includeMessageScheduler: false);
        var context = CreateContext(serviceProvider);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => context.CancelScheduledMessageAsync(Guid.NewGuid()));

        Assert.Contains(typeof(IMessageScheduler).FullName!, exception.Message);
        Assert.Contains("UseDelayedPublishing", exception.Message);
    }

    private static SagaContext<TestSagaInstance, TestEvent> CreateContext(
        TestServiceProvider serviceProvider,
        CancellationToken cancellationToken = default,
        Guid? sagaCorrelationId = null)
    {
        return new SagaContext<TestSagaInstance, TestEvent>(
            new TestEvent(Guid.NewGuid()),
            new TestSagaInstance
            {
                CorrelationId = sagaCorrelationId ?? Guid.NewGuid()
            },
            cancellationToken,
            serviceProvider);
    }
}
