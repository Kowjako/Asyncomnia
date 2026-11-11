using System.Data;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Transactions;

/// <summary>
/// Exposes the database transaction that Outbox writes should participate in.
/// When no transaction is active, the Dapper Outbox writer performs a standalone write.
/// </summary>
public interface IOutboxTransactionAccessor
{
    /// <summary>
    /// Gets the currently active transaction, or <see langword="null"/> when the
    /// Outbox message should be written independently.
    /// </summary>
    IDbTransaction? CurrentTransaction { get; }
}
