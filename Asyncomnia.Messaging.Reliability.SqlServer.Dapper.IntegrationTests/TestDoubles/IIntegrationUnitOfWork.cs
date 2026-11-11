using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Transactions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.IntegrationTests.TestDoubles;

internal interface IIntegrationUnitOfWork : IInboundTransaction
{
    Task InsertOrderAsync(Guid id, string description, CancellationToken token = default);
}
