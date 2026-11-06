using System.Data;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Migrations;

internal sealed class SqlServerMigrator(IOptions<SqlServerOutboxOptions> options, ILogger<SqlServerMigrator> logger)
{
    private const string ComponentName = "Messaging.Reliability.SqlServer";

    private const string ResourcePrefix = "Asyncomnia.Messaging.Reliability.SqlServer.Migrations.Schema.";

    private const string ResourceSuffix = ".sql";

    private readonly SqlServerOutboxOptions _options = options.Value;

    public async Task MigrateAsync(CancellationToken token = default)
    {
        await using var connection = new SqlConnection(_options.ConnectionString);
        await connection.OpenAsync(token);

        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(token);

        await AcquireMigrationLockAsync(connection, transaction, token);
        await EnsureMigrationHistoryAsync(connection, transaction, token);

        var appliedMigrations = await GetAppliedMigrationsAsync(connection, transaction, token);

        var availableMigrations = DiscoverMigrations();

        foreach (var migration in availableMigrations)
        {
            if (appliedMigrations.Contains(migration.Id))
            {
                continue;
            }

            var script = await ReadMigrationScriptAsync(migration, token);

            logger.LogInformation("Applying Asyncomnia migration {MigrationId}", migration.Id);

            await ExecuteMigrationAsync(connection, transaction, script, token);
            await RecordMigrationAsync(connection, transaction, migration.Id, token);
        }

        await transaction.CommitAsync(token);
    }

    private static IReadOnlyList<SqlMigration> DiscoverMigrations()
    {
        var assembly = typeof(SqlServerMigrator).Assembly;

        var migrations = assembly
            .GetManifestResourceNames()
            .Where(name => name.StartsWith(ResourcePrefix, StringComparison.Ordinal) &&
                           name.EndsWith(ResourceSuffix, StringComparison.OrdinalIgnoreCase))
            .Select(resourceName =>
            {
                var migrationId = resourceName[ResourcePrefix.Length..^ResourceSuffix.Length];

                ValidateMigrationId(migrationId);

                return new SqlMigration(migrationId, resourceName);
            })
            .OrderBy(migration => migration.Id, StringComparer.Ordinal)
            .ToArray();

        if (migrations.Length == 0)
        {
            throw new InvalidOperationException("No embedded Asyncomnia SQL migrations were found.");
        }

        return migrations;
    }

    private static void ValidateMigrationId(string migrationId)
    {
        var isValid =
            migrationId.Length > 5 &&
            migrationId[4] == '_' &&
            migrationId[..4].All(char.IsAsciiDigit);

        if (!isValid)
        {
            throw new InvalidOperationException($"Migration '{migrationId}' does not follow the '0001_MigrationName' naming convention.");
        }
    }

    private static async Task AcquireMigrationLockAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
    {
        const string sql = @"
            DECLARE @LockResult int;

            EXEC @LockResult = sys.sp_getapplock
                @Resource = N'Asyncomnia.Migrations',
                @LockMode = N'Exclusive',
                @LockOwner = N'Transaction',
                @LockTimeout = 30000;

            IF @LockResult < 0
                THROW 51000, 'Could not acquire the Asyncomnia migration lock', 1";

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        command.CommandTimeout = 35;

        await command.ExecuteNonQueryAsync(token);
    }

    private static async Task EnsureMigrationHistoryAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
    {
        const string sql = @"
            IF SCHEMA_ID(N'asyncomnia') IS NULL
            BEGIN
                EXEC(N'CREATE SCHEMA [asyncomnia]');
            END;

            IF OBJECT_ID(N'[asyncomnia].[SchemaMigrations]', N'U') IS NULL
            BEGIN
                CREATE TABLE [asyncomnia].[SchemaMigrations]
                (
                    [Component] nvarchar(200) NOT NULL,
                    [MigrationId] nvarchar(200) NOT NULL,
                    [AppliedAt] datetimeoffset(7) NOT NULL,

                    CONSTRAINT [PK_SchemaMigrations] PRIMARY KEY CLUSTERED
                    (
                        [Component],
                        [MigrationId]
                    )
                );
            END";

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;

        await command.ExecuteNonQueryAsync(token);
    }

    private static async Task<HashSet<string>> GetAppliedMigrationsAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
    {
        const string sql = @"
            SELECT [MigrationId]
            FROM [asyncomnia].[SchemaMigrations]
            WHERE [Component] = @Component";

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;

        command.Parameters.Add("@Component", SqlDbType.NVarChar, 200).Value = ComponentName;

        await using var reader = await command.ExecuteReaderAsync(token);

        var migrations = new HashSet<string>(StringComparer.Ordinal);

        while (await reader.ReadAsync(token))
        {
            migrations.Add(reader.GetString(0));
        }

        return migrations;
    }

    private static async Task ExecuteMigrationAsync(SqlConnection connection, SqlTransaction transaction, string script, CancellationToken token)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = script;
        command.CommandTimeout = 60;

        await command.ExecuteNonQueryAsync(token);
    }

    private static async Task RecordMigrationAsync(SqlConnection connection, SqlTransaction transaction, string migrationId, CancellationToken token)
    {
        const string sql = @"
            INSERT INTO [asyncomnia].[SchemaMigrations]
            (
                [Component],
                [MigrationId],
                [AppliedAt]
            )
            VALUES
            (
                @Component,
                @MigrationId,
                SYSUTCDATETIME()
            )";

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;

        command.Parameters.Add("@Component", SqlDbType.NVarChar, 200).Value = ComponentName;
        command.Parameters.Add("@MigrationId", SqlDbType.NVarChar, 200).Value = migrationId;

        await command.ExecuteNonQueryAsync(token);
    }

    private static async Task<string> ReadMigrationScriptAsync(SqlMigration migration, CancellationToken token)
    {
        var assembly = typeof(SqlServerMigrator).Assembly;

        await using var stream = assembly.GetManifestResourceStream(migration.ResourceName)
            ?? throw new InvalidOperationException($"Migration resource '{migration.ResourceName}' was not found.");

        using var reader = new StreamReader(stream);
        var script = await reader.ReadToEndAsync(token);

        if (string.IsNullOrWhiteSpace(script))
        {
            throw new InvalidOperationException($"Migration '{migration.Id}' is empty.");
        }

        return script;
    }
}