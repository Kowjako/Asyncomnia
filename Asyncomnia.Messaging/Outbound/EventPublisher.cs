using System;
using System.Collections.Generic;
using System.Text;

namespace Asyncomnia.Messaging.Outbound;

internal sealed class EventPublisher(MessageEnvelopeFactory factory, IOutboundMessagePipeline pipe) : IEventPublisher
{
    public Task PublishAsync<TEvent>(TEvent @event, MessageDispatchOptions? options, CancellationToken token = default)
        where TEvent : class, IEvent
    {
        var envelope = factory.Create<TEvent>(@event, options);
        return pipe.PublishAsync(envelope, token);
    }
}
