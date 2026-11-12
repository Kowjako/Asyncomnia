using System.Data;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.TestDoubles;

internal interface ITestUnitOfWork : IDbTransactionAccessor
{ }

internal sealed class TestUnitOfWork : ITestUnitOfWork
{
    public IDbTransaction? CurrentTransaction { get; set; }
}
