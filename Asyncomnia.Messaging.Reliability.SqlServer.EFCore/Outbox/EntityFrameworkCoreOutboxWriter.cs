using Asyncomnia.Messaging.Reliability.Outbox;
using Microsoft.EntityFrameworkCore;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Outbox;

internal sealed class EntityFrameworkCoreOutboxWriter<TDbContext> : IOutboxWriter where TDbContext : DbContext
{
    private readonly DbSet<OutboxEntry> _outboxEntries;

    public EntityFrameworkCoreOutboxWriter(TDbContext dbContext)
    {
        ArgumentNullException.ThrowIfNull(dbContext);

        if (dbContext.Model.FindEntityType(typeof(OutboxEntry)) is null)
        {
            throw new InvalidOperationException(
                "Asyncomnia Outbox is not configured in the EF Core model. " +
                "Call modelBuilder.AddAsyncomniaOutbox() in OnModelCreating.");
        }

        _outboxEntries = dbContext.Set<OutboxEntry>();
    }

    public Task WriteAsync(OutboxMessage message, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        token.ThrowIfCancellationRequested();

        _outboxEntries.Add(new OutboxEntry(message));

        return Task.CompletedTask;
    }
}