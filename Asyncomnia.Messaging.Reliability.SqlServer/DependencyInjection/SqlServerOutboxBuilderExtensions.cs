using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Migrations;
using Asyncomnia.Messaging.Reliability.SqlServer.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Storage;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;

public static class SqlServerOutboxBuilderExtensions
{
    public static OutboxBuilder UseSqlServer(this OutboxBuilder builder, Action<SqlServerOutboxOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<SqlServerOutboxOptions>, SqlServerOutboxOptionsValidator>());
        builder.Services.AddOptions<SqlServerOutboxOptions>().Configure(configure).ValidateOnStart();

        builder.Services.TryAddScoped<IOutboxStore, SqlServerOutboxStore>();
        builder.Services.TryAddScoped<SqlServerMigrator>();
        builder.Services.TryAddScoped<IDbTransactionAccessor, StandaloneDbTransactionAccessor>();
        builder.Services.TryAddScoped<IOutboxWriter, SqlServerOutboxWriter>();

        return builder;
    }

    /// <summary>
    /// Configures SQL Server persistence and makes Outbox writes participate in
    /// the transaction exposed by the application's scoped transaction accessor.
    /// When no transaction is active, the message is written immediately using
    /// a standalone database connection.
    /// </summary>
    /// <example>
    /// <code><![CDATA[
    /// services.AddScoped<IApplicationUnitOfWork, ApplicationUnitOfWork>();
    ///
    /// services
    ///     .AddAsyncomniaMessaging()
    ///     .UseOutbox(outbox => outbox.UseSqlServer<IApplicationUnitOfWork>(options =>
    ///     {
    ///         options.ConnectionString = connectionString;
    ///     }));
    /// ]]></code>
    /// </example>
    public static OutboxBuilder UseSqlServer<TTransactionAccessor>(this OutboxBuilder builder, Action<SqlServerOutboxOptions> configure)
        where TTransactionAccessor : class, IDbTransactionAccessor
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.UseSqlServer(configure);

        builder.Services.RemoveAll<IDbTransactionAccessor>();
        builder.Services.AddScoped<IDbTransactionAccessor>(services => services.GetRequiredService<TTransactionAccessor>());

        return builder;
    }
}
