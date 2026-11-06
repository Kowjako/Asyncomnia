using Asyncomnia.Messaging.Transport;
using EasyNetQ;

namespace Asyncomnia.Messaging.RabbitMQ.Transport;

internal sealed class EasyNetQOutboundTransport(IPubSub pubSub) : IOutboundTransport
{
    private readonly IPubSub _pubSub = pubSub ?? throw new ArgumentNullException(nameof(pubSub));

    public Task DispatchAsync(IMessage message, MessageMetadata metadata, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(metadata);

        token.ThrowIfCancellationRequested();

        var headers = RabbitMqHeaders.Create(metadata);

        return _pubSub.PublishAsync(
            message,
            message.GetType(),
            configuration => configuration.WithHeaders(headers),
            token);
    }
}