namespace Asyncomnia.Messaging.Reliability.Outbox.Configuration;

public sealed class OutboxOptions
{
    /// <summary>
    /// Number of workers for processing outbox messages table.
    /// If Default BatchSize = 10, then with 2 workers, we can process up to 20 outbox
    /// messages at a time
    /// </summary>
    public int WorkerCount { get; set; } = 1;

    public int BatchSize { get; set; } = 10;

    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(1);

    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    public int MaxAttempts { get; set; } = 10;
}