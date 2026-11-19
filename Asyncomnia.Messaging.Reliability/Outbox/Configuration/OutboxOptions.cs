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

    public TimeSpan RetryDelay { get; private set; } = TimeSpan.FromSeconds(5);

    public int MaxAttempts { get; private set; } = 10;

    public bool CleanupEnabled { get; private set; } = true;

    public TimeSpan ProcessedMessageRetention { get; private set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Retention period for permanently failed messages.
    /// Null means that failed messages are never automatically removed.
    /// </summary>
    public TimeSpan? FailedMessageRetention { get; private set; }

    public int CleanupBatchSize { get; private set; } = 1000;

    public TimeSpan CleanupInterval { get; private set; } = TimeSpan.FromMinutes(1);

    public void ConfigureRetryPolicy(int maxAttempts, TimeSpan delay)
    {
        MaxAttempts = maxAttempts;
        RetryDelay = delay;
    }

    public void ConfigureCleanup(TimeSpan processedRetention, TimeSpan? failedRetention, TimeSpan interval, int batchSize)
    {
        CleanupEnabled = true;
        ProcessedMessageRetention = processedRetention;
        FailedMessageRetention = failedRetention;
        CleanupInterval = interval;
        CleanupBatchSize = batchSize;
    }

    public void DisableCleanup()
    {
        CleanupEnabled = false;
    }
}