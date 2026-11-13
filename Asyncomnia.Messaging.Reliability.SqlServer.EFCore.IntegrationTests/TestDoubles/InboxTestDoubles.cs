using Asyncomnia.Messaging.Handlers;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.TestDoubles;

internal sealed record InboxOrderCreatedEvent(Guid OrderId, string Description) : IEvent;

internal sealed class InboxHandlerProbe
{
    private int _attempts;

    public int Attempts => Volatile.Read(ref _attempts);

    public bool ThrowAfterChanges { get; set; }

    public bool PublishOutboxMessage { get; set; }

    public void RecordAttempt()
    {
        Interlocked.Increment(ref _attempts);
    }
}

internal sealed class InboxOrderCreatedEventHandler(
    IntegrationDbContext dbContext,
    IEventPublisher publisher,
    InboxHandlerProbe probe) : IEventHandler<InboxOrderCreatedEvent>
{
    public async Task HandleAsync(
        InboxOrderCreatedEvent message,
        MessageMetadata metadata,
        CancellationToken ct)
    {
        probe.RecordAttempt();
        dbContext.Orders.Add(new TestOrder(message.OrderId, message.Description));

        if (probe.PublishOutboxMessage)
        {
            await publisher.PublishAsync(new OrderCreatedEvent(message.OrderId), token: ct);
        }

        if (probe.ThrowAfterChanges)
        {
            throw new InvalidOperationException("Simulated handler failure.");
        }
    }
}
