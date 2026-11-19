using Asyncomnia.Messaging.Reliability.SqlServer.Infrastructure.Connection;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Infrastructure;

internal sealed class EntityFrameworkCoreSqlServerConnectionFactory<TDbContext>(TDbContext dbContext)
    : ISqlServerConnectionFactory where TDbContext : DbContext
{
    private readonly TDbContext _dbContext = dbContext
        ?? throw new ArgumentNullException(nameof(dbContext));

    public async ValueTask<SqlConnection> OpenConnectionAsync(CancellationToken token = default)
    {
        var connectionString = _dbContext.Database.GetConnectionString();

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException($"DbContext '{typeof(TDbContext).FullName}' does not contain a SQL Server connection string.");
        }

        var connection = new SqlConnection(connectionString);

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
