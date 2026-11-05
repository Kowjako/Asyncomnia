using Microsoft.Extensions.Logging;

namespace Asyncomnia.Messaging.Reliability.Tests.TestDoubles;

internal sealed class CapturingLogger<T> : ILogger<T>
{
    public List<CapturedLogEntry> Entries { get; } = [];

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        return null;
    }

    public bool IsEnabled(LogLevel logLevel)
    {
        return true;
    }

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        Entries.Add(new CapturedLogEntry(logLevel, formatter(state, exception), exception));
    }
}

internal sealed record CapturedLogEntry(LogLevel LogLevel, string Message, Exception? Exception);
