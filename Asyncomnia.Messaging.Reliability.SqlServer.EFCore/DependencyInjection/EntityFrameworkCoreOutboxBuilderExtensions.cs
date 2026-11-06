using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Outbox;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DependencyInjection;

public static class EntityFrameworkCoreOutboxBuilderExtensions
{
    public static OutboxBuilder UseEntityFrameworkCore<TDbContext>(this OutboxBuilder builder) where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.TryAddScoped<IOutboxWriter, EntityFrameworkCoreOutboxWriter<TDbContext>>();

        return builder;
    }
}