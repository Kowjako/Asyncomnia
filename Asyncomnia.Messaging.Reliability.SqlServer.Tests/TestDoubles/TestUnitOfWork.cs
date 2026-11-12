using System.Data;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.TestDoubles;

internal interface ITestUnitOfWork : IInboundTransaction
{ }

internal sealed class TestUnitOfWork : ITestUnitOfWork
{
    public IDbTransaction? CurrentTransaction { get; set; }

    public Task BeginAsync(CancellationToken token = default)
    {
        return Task.CompletedTask;
    }

    public Task CommitAsync(CancellationToken token = default)
    {
        return Task.CompletedTask;
    }

    public Task RollbackAsync(CancellationToken token = default)
    {
        return Task.CompletedTask;
    }
}
