using System.Data;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Inbox;

internal sealed class EntityFrameworkCoreInboundTransaction<TDbContext>(TDbContext dbContext) : IInboundTransaction, IDbTransactionAccessor
    where TDbContext : DbContext
{
    public IDbTransaction? CurrentTransaction => dbContext.Database.CurrentTransaction?.GetDbTransaction();

    public async Task BeginAsync(CancellationToken token = default)
    {
        await dbContext.Database.BeginTransactionAsync(token);
    }

    public async Task CommitAsync(CancellationToken token = default)
    {
        await dbContext.SaveChangesAsync(token);
        await dbContext.Database.CommitTransactionAsync(token);
    }

    public Task RollbackAsync(CancellationToken token = default)
    {
        return dbContext.Database.RollbackTransactionAsync(token);
    }
}
