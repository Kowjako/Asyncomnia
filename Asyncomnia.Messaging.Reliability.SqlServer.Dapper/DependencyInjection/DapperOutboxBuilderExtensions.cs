using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Dapper.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.DependencyInjection;

public static class DapperOutboxBuilderExtensions
{
    /// <summary>
    /// Configures the Outbox to write messages through Dapper. When
    /// <typeparamref name="TTransactionAccessor"/> exposes an active transaction,
    /// the Outbox message participates in that transaction. Otherwise, the message
    /// is written immediately using a standalone database connection.
    /// The transaction accessor must already be registered as a scoped service.
    /// Asyncomnia resolves that same scoped instance, allowing domain changes and
    /// Outbox messages to be written atomically when a transaction is active.
    /// </summary>
    /// <typeparam name="TTransactionAccessor">
    /// The registered unit of work or transaction accessor that exposes the current
    /// database transaction, if any, through <see cref="IOutboxTransactionAccessor"/>.
    /// </typeparam>
    /// <param name="builder">
    /// The Outbox builder being configured.
    /// </param>
    /// <returns>
    /// The same <see cref="OutboxBuilder"/> instance.
    /// </returns>
    /// <example>
    /// <code><![CDATA[
    /// public interface IApplicationUnitOfWork : IOutboxTransactionAccessor
    /// {
    ///     Task BeginAsync(CancellationToken token = default);
    ///     Task CommitAsync(CancellationToken token = default);
    /// }
    ///
    /// services.AddScoped<IApplicationUnitOfWork, ApplicationUnitOfWork>();
    ///
    /// services
    ///     .AddAsyncomniaMessaging()
    ///     .UseOutbox(outbox =>
    ///     {
    ///         outbox.UseSqlServer(options =>
    ///         {
    ///             options.ConnectionString = connectionString;
    ///         })
    ///         .UseDapper<IApplicationUnitOfWork>();
    ///     });
    /// ]]></code>
    /// </example>
    public static OutboxBuilder UseDapper<TTransactionAccessor>(this OutboxBuilder builder) where TTransactionAccessor : class, IOutboxTransactionAccessor
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.TryAddScoped<IOutboxTransactionAccessor>(_ => _.GetRequiredService<TTransactionAccessor>());
        builder.Services.TryAddScoped<IOutboxWriter, DapperOutboxWriter>();

        return builder;
    }
}
