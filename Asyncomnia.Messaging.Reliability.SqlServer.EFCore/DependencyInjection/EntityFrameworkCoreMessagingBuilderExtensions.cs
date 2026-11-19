using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Infrastructure;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Outbox;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DependencyInjection;

public static class EntityFrameworkCoreMessagingBuilderExtensions
{
    public static SqlServerBuilder UseEntityFrameworkCore<TDbContext>(this SqlServerBuilder builder)
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.TryUseConnectionFactory<EntityFrameworkCoreSqlServerConnectionFactory<TDbContext>>();

        builder.Services.RemoveAll<IOutboxWriter>();
        builder.Services.AddScoped<IOutboxWriter, EntityFrameworkCoreOutboxWriter<TDbContext>>();

        builder.Services.RemoveAll<IDelayedMessageWriter>();
        builder.Services.AddScoped<IDelayedMessageWriter, EntityFrameworkCoreDelayedMessageWriter<TDbContext>>();

        return builder.UseInboundTransaction<EntityFrameworkCoreInboundTransaction<TDbContext>>();
    }
}