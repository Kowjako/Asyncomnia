namespace Asyncomnia.Messaging.Outbound;

internal sealed class EventPublisher(MessageEnvelopeFactory factory, IOutboundMessagePipeline pipe) : IEventPublisher
{
    public Task PublishAsync<TEvent>(TEvent @event, MessageDispatchOptions? options = null, CancellationToken token = default)
        where TEvent : class, IEvent
    {
        var envelope = factory.Create(@event, options);
        return pipe.PublishAsync(envelope, token);
    }
}