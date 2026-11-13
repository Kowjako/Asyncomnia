using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;

public static class AdoNetMessagingBuilderExtensions
{
    public static AsyncomniaMessagingBuilder UseAdoNet<TTransaction>(this AsyncomniaMessagingBuilder builder)
        where TTransaction : class, IInboundTransaction, IDbTransactionAccessor
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.RemoveAll<IOutboxWriter>();
        builder.Services.AddScoped<IOutboxWriter, SqlServerOutboxWriter>();

        return builder.UseInboundTransaction<TTransaction>();
    }
}