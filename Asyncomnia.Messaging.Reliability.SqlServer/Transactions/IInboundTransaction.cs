using Asyncomnia.Messaging.Reliability.Inbox;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Transactions;

/// <summary>
/// Controls the database transaction used while processing an inbound message.
/// The transaction includes the Inbox record, message handler, business changes
/// and Outbox messages produced by the handler.
/// </summary>
public interface IInboundTransaction : IInboxTransaction, IDbTransactionAccessor
{ 
}
