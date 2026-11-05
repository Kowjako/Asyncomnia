using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Storage;
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

        return builder;
    }
}