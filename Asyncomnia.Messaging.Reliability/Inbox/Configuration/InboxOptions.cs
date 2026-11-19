namespace Asyncomnia.Messaging.Reliability.Inbox.Configuration;

public sealed class InboxOptions
{
    public TimeSpan DuplicateDetectionWindow { get; set; } = TimeSpan.FromMinutes(30);

    public bool CleanupEnabled { get; private set; } = true;

    public int CleanupBatchSize { get; private set; } = 100;

    public TimeSpan CleanupInterval { get; private set; } = TimeSpan.FromSeconds(10);

    public void ConfigureCleanup(TimeSpan interval, int batchSize)
    {
        CleanupEnabled = true;
        CleanupInterval = interval;
        CleanupBatchSize = batchSize;
    }

    public void DisableCleanup()
    {
        CleanupEnabled = false;
    }
}