using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Extensions;
using Microsoft.EntityFrameworkCore;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.TestDoubles;

internal sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
{
    public int SaveChangesAsyncCallCount { get; private set; }

    public override Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        SaveChangesAsyncCallCount++;
        return base.SaveChangesAsync(cancellationToken);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.AddAsyncomniaOutbox();
        modelBuilder.AddAsyncomniaDelayedPublishing();
    }
}

internal sealed class UnconfiguredDbContext(DbContextOptions<UnconfiguredDbContext> options) : DbContext(options);
