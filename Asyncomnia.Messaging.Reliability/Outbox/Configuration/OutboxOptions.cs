namespace Asyncomnia.Messaging.Reliability.Outbox.Configuration;

public sealed class OutboxOptions
{
    public int WorkerCount { get; set; } = 1;
    public int BatchSize { get; set; } = 10;

    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(1);

    public int MaxAttempts { get; set; } = 10;
}