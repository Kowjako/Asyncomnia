using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Migrations;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;

public static class SqlServerInboxBuilderExtensions
{
    public static InboxBuilder UseSqlServer<TInboundTransaction>(this InboxBuilder builder, Action<SqlServerOptions>? configure = null)
        where TInboundTransaction: class, IInboundTransaction
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<SqlServerOptions>, SqlServerOptionsValidator>());
        var optionsBuilder = builder.Services.AddOptions<SqlServerOptions>();

        if (configure is not null)
        {
            optionsBuilder.Configure(configure);
        }

        optionsBuilder.ValidateOnStart();

        builder.Services.TryAddScoped<SqlServerMigrator>();

        builder.Services.RemoveAll<IInboundTransaction>();
        builder.Services.RemoveAll<IInboxTransaction>();
        builder.Services.RemoveAll<IDbTransactionAccessor>();

        builder.Services.AddScoped<IInboundTransaction>(services => services.GetRequiredService<TInboundTransaction>());
        builder.Services.AddScoped<IInboxTransaction>(services => services.GetRequiredService<TInboundTransaction>());
        builder.Services.AddScoped<IDbTransactionAccessor>(services => services.GetRequiredService<TInboundTransaction>());

        builder.Services.TryAddScoped<IInboxWriter, SqlServerInboxWriter>();
        return builder;
    }
}
