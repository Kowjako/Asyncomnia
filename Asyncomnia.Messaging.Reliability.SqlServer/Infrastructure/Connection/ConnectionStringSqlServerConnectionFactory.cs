using Microsoft.Data.SqlClient;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Infrastructure.Connection;

internal sealed class ConnectionStringSqlServerConnectionFactory(string connectionString) : ISqlServerConnectionFactory
{
    private readonly string _connectionString = !string.IsNullOrWhiteSpace(connectionString)
        ? connectionString
        : throw new ArgumentException("SQL Server connection string cannot be empty.", nameof(connectionString));

    public async ValueTask<SqlConnection> OpenConnectionAsync(CancellationToken token = default)
    {
        var connection = new SqlConnection(_connectionString);

        try
        {
            await connection.OpenAsync(token);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }
}
