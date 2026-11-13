using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;

internal static class InboundTransactionBuilderExtensions
{
    internal static AsyncomniaMessagingBuilder UseInboundTransaction<TTransaction>(this AsyncomniaMessagingBuilder builder)
        where TTransaction : class, IInboundTransaction, IDbTransactionAccessor
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.RemoveAll<IInboundTransaction>();
        builder.Services.RemoveAll<IDbTransactionAccessor>();

        builder.Services.AddScoped<IInboundTransaction>(services => services.GetRequiredService<TTransaction>());
        builder.Services.AddScoped<IDbTransactionAccessor>(services => services.GetRequiredService<TTransaction>());

        return builder;
    }
}