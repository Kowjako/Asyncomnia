using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.Inbox.Storage;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Migrations;
using Asyncomnia.Messaging.Reliability.SqlServer.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Outbox.Retention;
using Asyncomnia.Messaging.Reliability.SqlServer.Sagas;
using Asyncomnia.Messaging.Reliability.SqlServer.Sagas.Retention;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Asyncomnia.Messaging.Sagas.Persistence;
using Asyncomnia.Messaging.Sagas.Persistence.Retention;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;

public static class SqlServerMessagingBuilderExtensions
{
    public static AsyncomniaMessagingBuilder UseSqlServer(this AsyncomniaMessagingBuilder builder, Action<SqlServerBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var sqlServer = new SqlServerBuilder(builder.Services);

        // Configure adapters first so their registrations take precedence over defaults.
        configure(sqlServer);

        sqlServer.EnsureConnectionSourceConfigured();

        builder.Services.TryAddScoped<SqlServerMigrator>();

        builder.Services.TryAddScoped<IOutboxStore, SqlServerOutboxStore>();
        builder.Services.TryAddScoped<IOutboxWriter, SqlServerOutboxWriter>();
        builder.Services.TryAddScoped<IInboxWriter, SqlServerInboxWriter>();

        builder.Services.TryAddScoped<IDbTransactionAccessor, StandaloneDbTransactionAccessor>();

        builder.Services.TryAddScoped<IInboxCleanupStore, SqlServerInboxCleanupStore>();
        builder.Services.TryAddScoped<IDelayedMessageWriter, SqlServerDelayedMessageWriter>();
        builder.Services.TryAddScoped<IDelayedMessageStore, SqlServerDelayedMessageStore>();

        builder.Services.TryAddScoped(typeof(ISagaRepository<>), typeof(SqlServerSagaRepository<>));
        builder.Services.TryAddScoped<ISagaCleanupStore, SqlServerSagaCleanupStore>();

        builder.Services.TryAddScoped<IOutboxCleanupStore, SqlServerOutboxCleanupStore>();

        return builder;
    }
}
