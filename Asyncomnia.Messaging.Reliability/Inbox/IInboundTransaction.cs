namespace Asyncomnia.Messaging.Reliability.Inbox;

public interface IInboundTransaction
{
    Task BeginAsync(CancellationToken token = default);

    Task CommitAsync(CancellationToken token = default);

    Task RollbackAsync(CancellationToken token = default);
}