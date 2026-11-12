using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.TestDoubles;

internal interface IIntegrationUnitOfWork : IInboundTransaction
{
    Task InsertOrderAsync(Guid id, string description, CancellationToken token = default);
}
