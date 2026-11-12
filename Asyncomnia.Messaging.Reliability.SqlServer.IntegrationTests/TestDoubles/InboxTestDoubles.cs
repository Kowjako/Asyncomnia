using Asyncomnia.Messaging.Handlers;

namespace Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.TestDoubles;

internal sealed record InboxOrderCreatedEvent(Guid OrderId, string Description) : IEvent;

internal sealed class InboxHandlerProbe
{
    private int _attempts;

    public int Attempts => Volatile.Read(ref _attempts);

    public bool ThrowAfterInsert { get; set; }

    public bool PublishOutboxMessage { get; set; }

    public void RecordAttempt()
    {
        Interlocked.Increment(ref _attempts);
    }
}

internal sealed class InboxOrderCreatedEventHandler(
    IIntegrationUnitOfWork unitOfWork,
    IEventPublisher publisher,
    InboxHandlerProbe probe) : IEventHandler<InboxOrderCreatedEvent>
{
    public async Task HandleAsync(
        InboxOrderCreatedEvent message,
        MessageMetadata metadata,
        CancellationToken ct)
    {
        probe.RecordAttempt();

        await unitOfWork.InsertOrderAsync(message.OrderId, message.Description, ct);

        if (probe.PublishOutboxMessage)
        {
            await publisher.PublishAsync(new OrderCreatedEvent(message.OrderId), token: ct);
        }

        if (probe.ThrowAfterInsert)
        {
            throw new InvalidOperationException("Simulated handler failure.");
        }
    }
}
