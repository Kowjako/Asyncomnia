namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Transactions;

/// <summary>
/// Controls the database transaction used while processing an inbound message.
/// The transaction includes the Inbox record, message handler, business changes
/// and Outbox messages produced by the handler.
/// </summary>
public interface IInboundTransaction : IDbTransactionAccessor
{
    Task BeginAsync(CancellationToken token = default);

    Task CommitAsync(CancellationToken token = default);

    Task RollbackAsync(CancellationToken token = default);
}