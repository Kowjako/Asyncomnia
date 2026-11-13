using System.Data;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Transactions;

/// <summary>
/// Exposes the database transaction that messaging persistence operations
/// should participate in.
/// </summary>
public interface IDbTransactionAccessor
{
    /// <summary>
    /// Gets the currently active transaction, or <see langword="null"/>
    /// when no application transaction is active.
    /// </summary>
    IDbTransaction? CurrentTransaction { get; }
}
