using System.Data;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Transactions;

internal sealed class StandaloneDbTransactionAccessor : IDbTransactionAccessor
{
    public IDbTransaction? CurrentTransaction => null;
}