using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Asyncomnia.Messaging.Sagas.Tests.TestDoubles;

internal sealed class ManualTimeProvider : TimeProvider
{
    private readonly ConcurrentQueue<ManualTimer> _timers = new();
    private readonly SemaphoreSlim _timerCreated = new(0);

    public override ITimer CreateTimer(
        TimerCallback callback,
        object? state,
        TimeSpan dueTime,
        TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);

        var timer = new ManualTimer(callback, state);
        _timers.Enqueue(timer);
        _timerCreated.Release();

        return timer;
    }

    public Task WaitForTimerAsync(CancellationToken token = default)
    {
        return _timerCreated.WaitAsync(token);
    }

    public void FireNextTimer()
    {
        while (_timers.TryDequeue(out var timer))
        {
            if (timer.Fire())
            {
                return;
            }
        }

        throw new InvalidOperationException("There is no active timer to fire");
    }

    private sealed class ManualTimer(
        TimerCallback callback,
        object? state) : ITimer
    {
        private TimerCallback? _callback = callback;

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            return Volatile.Read(ref _callback) is not null;
        }

        public bool Fire()
        {
            var callbackToInvoke = Interlocked.Exchange(ref _callback, null);
            if (callbackToInvoke is null)
            {
                return false;
            }

            callbackToInvoke(state);
            return true;
        }

        public void Dispose()
        {
            Interlocked.Exchange(ref _callback, null);
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}

internal sealed class CapturingLogger<T> : ILogger<T>
{
    public List<CapturedLogEntry> Entries { get; } = [];

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull
    {
        return null;
    }

    public bool IsEnabled(LogLevel logLevel)
    {
        return true;
    }

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        Entries.Add(new CapturedLogEntry(
            logLevel,
            formatter(state, exception),
            exception));
    }
}

internal sealed record CapturedLogEntry(
    LogLevel LogLevel,
    string Message,
    Exception? Exception);
