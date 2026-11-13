using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.TestDoubles;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.DelayedPublishing;

public sealed class DelayedMessageEntryConfigurationTests
{
    [Fact]
    public void AddAsyncomniaDelayedPublishing_ConfiguresDelayedTable()
    {
        using var context = CreateContext();
        var model = context.GetService<IDesignTimeModel>().Model;
        var entityType = model.FindEntityType(typeof(DelayedMessageEntry));

        Assert.NotNull(entityType);
        Assert.Equal("Delayed", entityType!.GetTableName());
        Assert.Equal("asyncomnia", entityType.GetSchema());
        Assert.True(entityType.IsTableExcludedFromMigrations());
        Assert.Equal("PK_Delayed", entityType.FindPrimaryKey()!.GetName());
    }

    [Fact]
    public void AddAsyncomniaDelayedPublishing_ConfiguresDelayedColumnsAndUniqueMessageId()
    {
        using var context = CreateContext();
        var model = context.GetService<IDesignTimeModel>().Model;
        var entityType = model.FindEntityType(typeof(DelayedMessageEntry));

        Assert.NotNull(entityType);
        Assert.Equal(
            ValueGenerated.Never,
            entityType!.FindProperty(nameof(DelayedMessageEntry.ScheduleId))!.ValueGenerated);
        Assert.Equal(
            ValueGenerated.Never,
            entityType.FindProperty(nameof(DelayedMessageEntry.MessageId))!.ValueGenerated);
        Assert.Equal(
            1000,
            entityType.FindProperty(nameof(DelayedMessageEntry.ContractType))!.GetMaxLength());
        Assert.Equal(
            "nvarchar(max)",
            entityType.FindProperty(nameof(DelayedMessageEntry.MessageJson))!.GetColumnType());
        Assert.Equal(
            "datetimeoffset(7)",
            entityType.FindProperty(nameof(DelayedMessageEntry.CreatedAt))!.GetColumnType());
        Assert.Equal(
            "datetimeoffset(7)",
            entityType.FindProperty(nameof(DelayedMessageEntry.DeliverAt))!.GetColumnType());

        var messageIdIndex = Assert.Single(entityType.GetIndexes());
        Assert.True(messageIdIndex.IsUnique);
        Assert.Equal("UQ_Delayed_MessageId", messageIdIndex.GetDatabaseName());
        Assert.Equal(
            nameof(DelayedMessageEntry.MessageId),
            Assert.Single(messageIdIndex.Properties).Name);
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
