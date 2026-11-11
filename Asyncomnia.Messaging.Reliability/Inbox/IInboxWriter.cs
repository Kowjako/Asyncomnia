namespace Asyncomnia.Messaging.Reliability.Inbox;

public interface IInboxWriter
{
    /// <summary>
    /// Attempts to write an Inbox message.
    /// Returns <see langword="false"/> when the message has already been
    /// processed by the same consumer.
    /// </summary>
    Task<bool> TryAddAsync(InboxMessage message, CancellationToken token = default);
}