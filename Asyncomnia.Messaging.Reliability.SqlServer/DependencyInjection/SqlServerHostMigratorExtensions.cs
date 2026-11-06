using Asyncomnia.Messaging.Reliability.SqlServer.Migrations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;

public static class SqlServerHostMigratorExtensions
{
    public static async Task MigrateAsyncomniaAsync(this IHost host, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(host);

        await using var scope = host.Services.CreateAsyncScope();
        var migrator = scope.ServiceProvider.GetRequiredService<SqlServerMigrator>();

        await migrator.MigrateAsync(token);
    }
}