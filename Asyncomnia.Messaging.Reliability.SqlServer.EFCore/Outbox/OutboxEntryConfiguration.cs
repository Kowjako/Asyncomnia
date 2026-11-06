using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Outbox;

internal sealed class OutboxEntryConfiguration : IEntityTypeConfiguration<OutboxEntry>
{
    public void Configure(EntityTypeBuilder<OutboxEntry> builder)
    {
        builder.ToTable(SqlServerOutboxDefaults.TableName, SqlServerOutboxDefaults.SchemaName,
            table => table.ExcludeFromMigrations());

        builder
            .HasKey(entry => entry.MessageId)
            .HasName("PK_Outbox");

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
            .Property(entry => entry.AvailableAt)
            .HasColumnType("datetimeoffset(7)")
            .IsRequired();
    }
}