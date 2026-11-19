using Asyncomnia.Messaging.Reliability.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.DependencyInjection;

internal static class InboundTransactionBuilderExtensions
{
    internal static SqlServerBuilder UseInboundTransaction<TTransaction>(this SqlServerBuilder builder)
        where TTransaction : class, IInboundTransaction, IDbTransactionAccessor
    {
        ArgumentNullException.ThrowIfNull(builder);

        var transactionType = typeof(TTransaction);
        var transactionIsRegistered = builder.Services.Any(descriptor => descriptor.ServiceType == transactionType);

        if (!transactionIsRegistered)
        {
            if (transactionType.IsInterface || transactionType.IsAbstract)
            {
                throw new InvalidOperationException($"Transaction type '{transactionType.FullName}' must be registered before calling UseAdoNet<{transactionType.Name}>().");
            }

            builder.Services.AddScoped<TTransaction>();
        }

        builder.Services.RemoveAll<IInboundTransaction>();
        builder.Services.RemoveAll<IDbTransactionAccessor>();

        builder.Services.AddScoped<IInboundTransaction>(services => services.GetRequiredService<TTransaction>());

        builder.Services.AddScoped<IDbTransactionAccessor>(services => services.GetRequiredService<TTransaction>());

        return builder;
    }
}
