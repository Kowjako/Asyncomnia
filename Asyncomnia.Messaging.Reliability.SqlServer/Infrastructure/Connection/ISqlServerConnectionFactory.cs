using Microsoft.Data.SqlClient;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Infrastructure.Connection;

internal interface ISqlServerConnectionFactory
{
    ValueTask<SqlConnection> OpenConnectionAsync(CancellationToken token = default);
}