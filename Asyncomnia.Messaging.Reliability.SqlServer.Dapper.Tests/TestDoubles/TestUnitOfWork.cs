using System.Data.Common;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Transactions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Tests.TestDoubles;

internal interface ITestUnitOfWork : IOutboxTransactionAccessor
{
}

internal sealed class TestUnitOfWork : ITestUnitOfWork
{
    public DbTransaction? CurrentTransaction { get; set; }
}
