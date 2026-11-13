namespace Asyncomnia.Messaging.Reliability.Inbox.Configuration;

public sealed class InboxOptions
{
    public TimeSpan DuplicateDetectionWindow { get; set; } = TimeSpan.FromMinutes(30);

    public int CleanupBatchSize { get; set; } = 100;

    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromSeconds(10);

    public bool CleanupEnabled { get; set; } = true;
}