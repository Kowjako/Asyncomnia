using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Transactions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.IntegrationTests.TestDoubles;

internal interface IIntegrationUnitOfWork : IOutboxTransactionAccessor
{
    Task BeginAsync(CancellationToken token = default);

    Task InsertOrderAsync(Guid id, string description, CancellationToken token = default);

    Task CommitAsync(CancellationToken token = default);

    Task RollbackAsync(CancellationToken token = default);
}
