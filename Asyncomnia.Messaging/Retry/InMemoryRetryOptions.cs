namespace Asyncomnia.Messaging.Retry;

public sealed class InMemoryRetryOptions
{
    private IReadOnlyList<TimeSpan>? _delays;

    internal IReadOnlyList<TimeSpan> Delays => _delays ?? Array.Empty<TimeSpan>();

    internal bool IsConfigured => _delays is not null;

    public void Immediately(int retryCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(retryCount, 1);

        SetStrategy(Enumerable.Repeat(TimeSpan.Zero, retryCount).ToArray());
    }

    public void Intervals(params TimeSpan[] intervals)
    {
        ArgumentNullException.ThrowIfNull(intervals);

        if (intervals.Length == 0)
        {
            throw new ArgumentException("At least one retry interval must be configured", nameof(intervals));
        }

        if (intervals.Any(interval => interval <= TimeSpan.Zero))
        {
            throw new ArgumentException("Retry intervals must be greater than zero", nameof(intervals));
        }

        SetStrategy(intervals.ToArray());
    }

    private void SetStrategy(IReadOnlyList<TimeSpan> delays)
    {
        if (_delays is not null)
        {
            throw new InvalidOperationException("In-memory retry strategy has already been configured.");
        }

        _delays = delays;
    }
}
