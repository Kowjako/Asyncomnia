using Asyncomnia.Messaging.Handlers;

namespace Asyncomnia.Messaging.Tests.TestDoubles;

internal sealed class InMemoryRetryHandlerProbe(
    int transientEventFailures = 2)
{
    public int SuccessfulEventCalls { get; set; }

    public int TransientEventCalls { get; set; }

    public int CommandCalls { get; set; }

    public Exception CommandException { get; } =
        new InvalidOperationException("command handler failed");

    public bool ShouldFailTransientEvent =>
        TransientEventCalls <= transientEventFailures;
}

internal sealed class SuccessfulRetryEventHandler(
    InMemoryRetryHandlerProbe probe) : IEventHandler<TestEvent>
{
    public Task HandleAsync(
        TestEvent message,
        MessageMetadata metadata,
        CancellationToken cancellationToken)
    {
        probe.SuccessfulEventCalls++;
        return Task.CompletedTask;
    }
}

internal sealed class TransientRetryEventHandler(
    InMemoryRetryHandlerProbe probe) : IEventHandler<TestEvent>
{
    public Task HandleAsync(
        TestEvent message,
        MessageMetadata metadata,
        CancellationToken cancellationToken)
    {
        probe.TransientEventCalls++;

        return probe.ShouldFailTransientEvent
            ? Task.FromException(
                new InvalidOperationException("event handler failed"))
            : Task.CompletedTask;
    }
}

internal sealed class FailingRetryCommandHandler(
    InMemoryRetryHandlerProbe probe) : ICommandHandler<TestCommand>
{
    public Task HandleAsync(
        TestCommand message,
        MessageMetadata metadata,
        CancellationToken cancellationToken)
    {
        probe.CommandCalls++;
        return Task.FromException(probe.CommandException);
    }
}
