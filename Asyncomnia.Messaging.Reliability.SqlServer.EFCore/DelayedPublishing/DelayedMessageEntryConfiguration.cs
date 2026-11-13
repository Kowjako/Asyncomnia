using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DelayedPublishing;

internal sealed class DelayedMessageEntryConfiguration : IEntityTypeConfiguration<DelayedMessageEntry>
{
    public void Configure(EntityTypeBuilder<DelayedMessageEntry> builder)
    {
        builder.ToTable(SqlServerDelayedDefaults.TableName, SqlServerDelayedDefaults.SchemaName,
            table => table.ExcludeFromMigrations());

        builder
            .HasKey(entry => entry.ScheduleId)
            .HasName("PK_Delayed");

        builder
            .Property(entry => entry.ScheduleId)
            .HasColumnType("uniqueidentifier")
            .ValueGeneratedNever();

        builder
            .Property(entry => entry.MessageId)
            .HasColumnType("uniqueidentifier")
            .ValueGeneratedNever();

        builder
            .Property(entry => entry.ContractType)
            .HasMaxLength(1000)
            .IsRequired();

        builder
            .Property(entry => entry.MessageJson)
            .HasColumnType("nvarchar(max)")
            .IsRequired();

        builder
            .Property(entry => entry.CorrelationId)
            .HasColumnType("uniqueidentifier");

        builder
            .Property(entry => entry.CreatedAt)
            .HasColumnType("datetimeoffset(7)")
            .IsRequired();

        builder
            .Property(entry => entry.DeliverAt)
            .HasColumnType("datetimeoffset(7)")
            .IsRequired();

        builder
            .HasIndex(entry => entry.MessageId)
            .IsUnique()
            .HasDatabaseName("UQ_Delayed_MessageId");
    }
}