using Asyncomnia.Messaging.Reliability.SqlServer.Infrastructure.Connection;
using Microsoft.Data.SqlClient;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.TestDoubles;

internal sealed class TestSqlServerConnectionFactory : ISqlServerConnectionFactory
{
    public ValueTask<SqlConnection> OpenConnectionAsync(CancellationToken token = default)
    {
        throw new InvalidOperationException("A database connection was not expected in this test.");
    }
}
