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
using Asyncomnia.Messaging.Reliability.SqlServer.Sagas;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Asyncomnia.Messaging.Sagas.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;

public static class SqlServerMessagingBuilderExtensions
{
    public static AsyncomniaMessagingBuilder UseSqlServer(this AsyncomniaMessagingBuilder builder, Action<SqlServerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<SqlServerOptions>, SqlServerOptionsValidator>());

        builder.Services.AddOptions<SqlServerOptions>().Configure(configure).ValidateOnStart();

        builder.Services.TryAddScoped<SqlServerMigrator>();

        builder.Services.TryAddScoped<IOutboxStore, SqlServerOutboxStore>();
        builder.Services.TryAddScoped<IOutboxWriter, SqlServerOutboxWriter>();

        builder.Services.TryAddScoped<IInboxWriter, SqlServerInboxWriter>();
        builder.Services.TryAddScoped<IDbTransactionAccessor, StandaloneDbTransactionAccessor>();

        builder.Services.TryAddScoped<IInboxCleanupStore, SqlServerInboxCleanupStore>();

        builder.Services.TryAddScoped<IDelayedMessageWriter, SqlServerDelayedMessageWriter>();
        builder.Services.TryAddScoped<IDelayedMessageStore, SqlServerDelayedMessageStore>();

        // open generic registration, when someone will ask for ISagaRepository<T> .net will automatically
        // close generic and will return appropriate class
        builder.Services.TryAddScoped(typeof(ISagaRepository<>), typeof(SqlServerSagaRepository<>));

        return builder;
    }
}