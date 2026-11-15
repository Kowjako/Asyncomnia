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
    public async Task SendAsync_ForwardsCommandOptionsAndCancellationToken()
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
    public async Task PublishAsync_ForwardsEventOptionsAndCancellationToken()
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

    private static SagaContext<TestSagaInstance, TestEvent> CreateContext(
        TestServiceProvider serviceProvider,
        CancellationToken cancellationToken = default)
    {
        return new SagaContext<TestSagaInstance, TestEvent>(
            new TestEvent(Guid.NewGuid()),
            new TestSagaInstance(),
            cancellationToken,
            serviceProvider);
    }
}
