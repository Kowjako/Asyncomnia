namespace Asyncomnia.Messaging.Sagas.Configuration;

public sealed class SagaOptions
{
    public bool CleanupEnabled { get; set; } = true;

    /// <summary>
    /// Specifies how long finalized saga instances are retained after their last update.
    /// This period should be longer than the Inbox duplicate detection window and the
    /// maximum expected delay of a late message.
    /// </summary>
    public TimeSpan FinalizedSagaRetention { get; set; } = TimeSpan.FromDays(30);

    public int CleanupBatchSize { get; set; } = 1000;

    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromMinutes(5);
}
