namespace Asyncomnia.Messaging.Reliability.DelayedPublishing.Configuration;

public sealed class DelayedPublishingOptions
{
    public int BatchSize { get; set; } = 100;

    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);
}