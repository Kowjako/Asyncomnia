using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Microsoft.EntityFrameworkCore;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DelayedPublishing;

internal sealed class EntityFrameworkCoreDelayedMessageWriter<TDbContext> : IDelayedMessageWriter where TDbContext : DbContext
{
    private readonly DbSet<DelayedMessageEntry> _delayedMessageEntries;
    private readonly TDbContext _dbContext;

    public EntityFrameworkCoreDelayedMessageWriter(TDbContext dbContext)
    {
        ArgumentNullException.ThrowIfNull(dbContext);

        if (dbContext.Model.FindEntityType(typeof(DelayedMessageEntry)) is null)
        {
            throw new InvalidOperationException("Asyncomnia Delayed Messaging is not configured in the EF Core model. Call modelBuilder.AddAsyncomniaDelayedPublishing() in OnModelCreating.");
        }

        _dbContext = dbContext;
        _delayedMessageEntries = dbContext.Set<DelayedMessageEntry>();
    }

    public async Task<bool> CancelAsync(Guid scheduleId, CancellationToken token = default)
    {
        if (scheduleId == Guid.Empty)
        {
            throw new ArgumentException("Schedule ID cannot be empty", nameof(scheduleId));
        }

        token.ThrowIfCancellationRequested();

        var trackedEntry = _dbContext.ChangeTracker.Entries<DelayedMessageEntry>()
                                                   .FirstOrDefault(entry => entry.Entity.ScheduleId == scheduleId);

        if (trackedEntry?.State == EntityState.Added)
        {
            trackedEntry.State = EntityState.Detached;
            return true;
        }

        if (trackedEntry?.State == EntityState.Deleted)
        {
            return false;
        }

        if (trackedEntry is not null)
        {
            trackedEntry.State = EntityState.Detached;
        }

        var deletedCount = await _delayedMessageEntries
            .Where(entry => entry.ScheduleId == scheduleId)
            .ExecuteDeleteAsync(token);

        return deletedCount == 1;
    }

    public Task WriteAsync(DelayedMessage message, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        token.ThrowIfCancellationRequested();

        _delayedMessageEntries.Add(new DelayedMessageEntry(message));

        return Task.CompletedTask;
    }
}
