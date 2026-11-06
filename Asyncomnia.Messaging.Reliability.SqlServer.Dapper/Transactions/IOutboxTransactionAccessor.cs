using System.Data.Common;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Transactions;

public interface IOutboxTransactionAccessor
{
    DbTransaction? CurrentTransaction { get; }
}