using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Extensions;
using Microsoft.EntityFrameworkCore;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.TestDoubles;

internal sealed class IntegrationDbContext(DbContextOptions<IntegrationDbContext> options) : DbContext(options)
{
    public DbSet<TestOrder> Orders => Set<TestOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TestOrder>(builder =>
        {
            builder.ToTable("Orders", "dbo");
            builder.HasKey(order => order.Id).HasName("PK_Orders");
            builder.Property(order => order.Id).ValueGeneratedNever();
            builder.Property(order => order.Description).HasMaxLength(200).IsRequired();
        });

        modelBuilder.AddAsyncomniaOutbox();
    }
}
