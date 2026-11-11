using System.Data;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Transactions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Tests.TestDoubles;

internal interface ITestUnitOfWork : IOutboxTransactionAccessor
{
}

internal sealed class TestUnitOfWork : ITestUnitOfWork
{
    public IDbTransaction? CurrentTransaction { get; set; }
}
