using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.TestDoubles;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.Outbox;

public sealed class OutboxEntryConfigurationTests
{
    [Fact]
    public void AddAsyncomniaOutbox_ConfiguresOutboxTable()
    {
        using var context = CreateContext();

        var model = context.GetService<IDesignTimeModel>().Model;
        var entityType = model.FindEntityType(typeof(OutboxEntry));

        Assert.NotNull(entityType);
        Assert.Equal("Outbox", entityType!.GetTableName());
        Assert.Equal("asyncomnia", entityType.GetSchema());
        Assert.True(entityType.IsTableExcludedFromMigrations());
    }

    [Fact]
    public void AddAsyncomniaOutbox_ConfiguresOutboxColumns()
    {
        using var context = CreateContext();
        var model = context.GetService<IDesignTimeModel>().Model;
        var entityType = model.FindEntityType(typeof(OutboxEntry));

        Assert.NotNull(entityType);
        Assert.Equal(
            ValueGenerated.Never,
            entityType!.FindProperty(nameof(OutboxEntry.MessageId))!.ValueGenerated);
        Assert.Equal(
            1000,
            entityType.FindProperty(nameof(OutboxEntry.ContractType))!.GetMaxLength());
        Assert.Equal(
            "nvarchar(max)",
            entityType.FindProperty(nameof(OutboxEntry.MessageJson))!.GetColumnType());
        Assert.Equal(
            "datetimeoffset(7)",
            entityType.FindProperty(nameof(OutboxEntry.CreatedAt))!.GetColumnType());
        Assert.Equal(
            "datetimeoffset(7)",
            entityType.FindProperty(nameof(OutboxEntry.AvailableAt))!.GetColumnType());
    }

    private static TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlServer(
                "Server=localhost;Database=AsyncomniaModelTests;" +
                "User Id=sa;Password=NotUsed123!;TrustServerCertificate=True")
            .Options;

        return new TestDbContext(options);
    }
}
