namespace Asyncomnia.Messaging.Reliability.Inbox;

public interface IInboxTransaction
{
    Task BeginAsync(CancellationToken token = default);

    Task CommitAsync(CancellationToken token = default);

    Task RollbackAsync(CancellationToken token = default);
}