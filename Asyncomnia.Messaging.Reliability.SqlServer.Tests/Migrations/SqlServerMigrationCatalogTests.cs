using Asyncomnia.Messaging.Reliability.SqlServer.Migrations;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.Migrations;

public sealed class SqlServerMigrationCatalogTests
{
    private const string ResourcePrefix =
        "Asyncomnia.Messaging.Reliability.SqlServer.Migrations.Schema.";

    [Fact]
    public void CreateMigrationCatalog_WithUnorderedResources_ReturnsMigrationsOrderedById()
    {
        var resourceNames = new[]
        {
            CreateResourceName("0004_CreateDelayed"),
            CreateResourceName("0003_CreateInboxCleanupIndex"),
            CreateResourceName("0002_CreateInbox"),
            "Asyncomnia.Unrelated.Resource.sql",
            CreateResourceName("0001_CreateOutbox")
        };

        var migrations = SqlServerMigrator.CreateMigrationCatalog(resourceNames);

        Assert.Collection(
            migrations,
            migration => Assert.Equal("0001_CreateOutbox", migration.Id),
            migration => Assert.Equal("0002_CreateInbox", migration.Id),
            migration => Assert.Equal("0003_CreateInboxCleanupIndex", migration.Id),
            migration => Assert.Equal("0004_CreateDelayed", migration.Id));
    }

    [Fact]
    public void CreateMigrationCatalog_WithInvalidMigrationId_ThrowsInvalidOperationException()
    {
        var resourceNames = new[]
        {
            CreateResourceName("01_CreateOutbox")
        };

        var exception = Assert.Throws<InvalidOperationException>(() =>
            SqlServerMigrator.CreateMigrationCatalog(resourceNames));

        Assert.Contains("0001_MigrationName", exception.Message);
    }

    [Fact]
    public void CreateMigrationCatalog_WithDuplicateVersion_ThrowsInvalidOperationException()
    {
        var resourceNames = new[]
        {
            CreateResourceName("0001_CreateOutbox"),
            CreateResourceName("0001_CreateInbox")
        };

        var exception = Assert.Throws<InvalidOperationException>(() =>
            SqlServerMigrator.CreateMigrationCatalog(resourceNames));

        Assert.Contains("Duplicate Asyncomnia migration versions: 0001", exception.Message);
    }

    [Fact]
    public void EmbeddedSqlResources_ContainsBootstrapAndAllMigrations()
    {
        var resourceNames = typeof(SqlServerMigrator)
            .Assembly
            .GetManifestResourceNames();

        Assert.Contains(
            "Asyncomnia.Messaging.Reliability.SqlServer.Migrations.Bootstrap.CreateMigrationHistory.sql",
            resourceNames);
        Assert.Contains(
            CreateResourceName("0001_CreateOutbox"),
            resourceNames);
        Assert.Contains(
            CreateResourceName("0002_CreateInbox"),
            resourceNames);
        Assert.Contains(
            CreateResourceName("0003_CreateInboxCleanupIndex"),
            resourceNames);
        Assert.Contains(
            CreateResourceName("0004_CreateDelayed"),
            resourceNames);
    }

    private static string CreateResourceName(string migrationId)
    {
        return $"{ResourcePrefix}{migrationId}.sql";
    }
}
