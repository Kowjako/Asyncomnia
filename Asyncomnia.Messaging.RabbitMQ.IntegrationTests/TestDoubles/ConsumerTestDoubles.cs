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

internal sealed class BrokerRetryProbe(int expectedAttempts)
{
    private readonly TaskCompletionSource<int> _expectedAttemptsReached =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private int _attempts;

    public int Attempts => Volatile.Read(ref _attempts);

    public void RecordFailure()
    {
        var attempts = Interlocked.Increment(ref _attempts);

        if (attempts == expectedAttempts)
        {
            _expectedAttemptsReached.TrySetResult(attempts);
        }
    }

    public Task<int> WaitForExpectedAttemptsAsync(CancellationToken ct)
    {
        return _expectedAttemptsReached.Task.WaitAsync(ct);
    }
}

internal sealed class BrokerRetryClientCreatedEventHandler(BrokerRetryProbe probe) : IEventHandler<ClientCreatedEvent>
{
    public Task HandleAsync(ClientCreatedEvent message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        probe.RecordFailure();

        return Task.FromException(
            new InvalidOperationException("Handler failed for broker retry integration test"));
    }
}

internal sealed class InMemoryRetryProbe(int failuresBeforeSuccess)
{
    private readonly TaskCompletionSource<int> _succeeded =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private int _attempts;

    public int Attempts => Volatile.Read(ref _attempts);

    public Task HandleAsync()
    {
        var attempts = Interlocked.Increment(ref _attempts);

        if (attempts <= failuresBeforeSuccess)
        {
            return Task.FromException(
                new InvalidOperationException(
                    "Handler failed for in-memory retry integration test"));
        }

        _succeeded.TrySetResult(attempts);
        return Task.CompletedTask;
    }

    public Task<int> WaitForSuccessAsync(CancellationToken ct)
    {
        return _succeeded.Task.WaitAsync(ct);
    }
}

internal sealed class InMemoryRetryClientCreatedEventHandler(
    InMemoryRetryProbe probe) : IEventHandler<ClientCreatedEvent>
{
    public Task HandleAsync(
        ClientCreatedEvent message,
        MessageMetadata metadata,
        CancellationToken cancellationToken)
    {
        return probe.HandleAsync();
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
