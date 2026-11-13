using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.TestDoubles;

internal interface IIntegrationUnitOfWork : IInboundTransaction, IDbTransactionAccessor
{
    Task InsertOrderAsync(Guid id, string description, CancellationToken token = default);
}
