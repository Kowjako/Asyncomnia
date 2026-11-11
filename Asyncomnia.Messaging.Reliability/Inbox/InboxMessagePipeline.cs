using Asyncomnia.Messaging.Inbound;

namespace Asyncomnia.Messaging.Reliability.Inbox;

internal sealed class InboxMessagePipeline(
    HandlerMessagePipeline next,
    IInboxWriter writer,
    IInboxTransaction transaction,
    TimeProvider timeProvider) : IInboundMessagePipeline
{
    private readonly HandlerMessagePipeline _next =
        next ?? throw new ArgumentNullException(nameof(next));

    private readonly IInboxWriter _writer =
        writer ?? throw new ArgumentNullException(nameof(writer));

    private readonly IInboxTransaction _transaction =
        transaction ?? throw new ArgumentNullException(nameof(transaction));

    private readonly TimeProvider _timeProvider =
        timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));

    public Task HandleEventAsync<TEvent>(InboundMessageContext context, CancellationToken token = default)
        where TEvent : class, IEvent
    {
        return HandleAsync(context, nextToken => _next.HandleEventAsync<TEvent>(context, nextToken), token);
    }

    public Task HandleCommandAsync<TCommand>(InboundMessageContext context, CancellationToken token = default)
        where TCommand : class, ICommand
    {
        return HandleAsync(context, nextToken => _next.HandleCommandAsync<TCommand>(context, nextToken), token);
    }

    private async Task HandleAsync(InboundMessageContext context, Func<CancellationToken, Task> next, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(context);

        var inboxMessage = new InboxMessage(
            context.Metadata.MessageId, 
            context.ConsumerId, 
            context.ContractType, 
            _timeProvider.GetUtcNow());

        await _transaction.BeginAsync(token);

        var committed = false;

        try
        {
            var written = await _writer.TryAddAsync(inboxMessage, token);

            if (!written)
            {
                return;
            }

            await next(token);

            await _transaction.CommitAsync(token);
            committed = true;
        }
        finally
        {
            if (!committed)
            {
                await _transaction.RollbackAsync(CancellationToken.None);
            }
        }
    }
}
