using Asyncomnia.Messaging.Handlers;

namespace Asyncomnia.Messaging.RabbitMQ.IntegrationTests.TestDoubles;

internal sealed record HandledMessage<TMessage>(TMessage Message, MessageMetadata Metadata);

internal sealed class MessageProbe<TMessage>
{
    private readonly TaskCompletionSource<HandledMessage<TMessage>> _handled =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Record(TMessage message, MessageMetadata metadata)
    {
        _handled.TrySetResult(new HandledMessage<TMessage>(message, metadata));
    }

    public Task<HandledMessage<TMessage>> WaitAsync(CancellationToken ct)
    {
        return _handled.Task.WaitAsync(ct);
    }
}

internal sealed class ClientCreatedEventHandler(MessageProbe<ClientCreatedEvent> probe) : IEventHandler<ClientCreatedEvent>
{
    public Task HandleAsync(ClientCreatedEvent message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        probe.Record(message, metadata);
        return Task.CompletedTask;
    }
}

internal sealed class FailingClientCreatedEventHandler : IEventHandler<ClientCreatedEvent>
{
    public Task HandleAsync(ClientCreatedEvent message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        return Task.FromException(
            new InvalidOperationException("Handler failed for integration test"));
    }
}

internal sealed class StartLongWorkJobCommandHandler(MessageProbe<StartLongWorkJobCommand> probe) : ICommandHandler<StartLongWorkJobCommand>
{
    public Task HandleAsync(StartLongWorkJobCommand message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        probe.Record(message, metadata);
        return Task.CompletedTask;
    }
}

internal sealed class BlockingLongWorkJobCommandHandler(ConcurrentCommandProbe probe) : ICommandHandler<StartLongWorkJobCommand>
{
    public Task HandleAsync(StartLongWorkJobCommand message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        return probe.HandleAsync(cancellationToken);
    }
}

internal sealed class ConcurrentCommandProbe
{
    private readonly TaskCompletionSource<bool> _twoStarted =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<bool> _allCompleted =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<bool> _release =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private int _active;
    private int _maximumActive;
    private int _started;
    private int _completed;

    public int MaximumActive => Volatile.Read(ref _maximumActive);

    public async Task HandleAsync(CancellationToken ct)
    {
        var active = Interlocked.Increment(ref _active);
        UpdateMaximumActive(active);

        if (Interlocked.Increment(ref _started) == 2)
        {
            _twoStarted.TrySetResult(true);
        }

        try
        {
            await _release.Task.WaitAsync(ct);
        }
        finally
        {
            Interlocked.Decrement(ref _active);

            if (Interlocked.Increment(ref _completed) == 2)
            {
                _allCompleted.TrySetResult(true);
            }
        }
    }

    public Task WaitUntilTwoStartedAsync(CancellationToken ct)
    {
        return _twoStarted.Task.WaitAsync(ct);
    }

    public Task WaitUntilAllCompletedAsync(CancellationToken ct)
    {
        return _allCompleted.Task.WaitAsync(ct);
    }

    public void Release()
    {
        _release.TrySetResult(true);
    }

    private void UpdateMaximumActive(int active)
    {
        while (true)
        {
            var currentMaximum = Volatile.Read(ref _maximumActive);

            if (active <= currentMaximum)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref _maximumActive, active, currentMaximum) == currentMaximum)
            {
                return;
            }
        }
    }
}
