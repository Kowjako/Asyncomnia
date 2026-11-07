using Asyncomnia.Messaging.Inbound;
using EasyNetQ;
using EasyNetQ.Consumer;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal sealed class RabbitMqMessageConsumer(IServiceScopeFactory scopeFactory) 
{
    private readonly IServiceScopeFactory _scopeFactory = scopeFactory
        ?? throw new ArgumentNullException(nameof(scopeFactory));

    public async Task<AckStrategyAsync> ConsumeAsync<TMessage>(
        IMessage<TMessage> message, 
        MessageReceivedInfo messageReceivedInfo,
        Func<IInboundMessagePipeline, MessageEnvelope<TMessage>, CancellationToken, Task> handle,
        CancellationToken ct) where TMessage: class, IMessage
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(handle);

        var messageMetadata = RabbitMqMessageMetadataMapper.Map(message.Properties);
        var envelope = new MessageEnvelope<TMessage>(message.Body, messageMetadata);

        await using var scope = _scopeFactory.CreateAsyncScope();

        var pipeline = scope.ServiceProvider.GetRequiredService<IInboundMessagePipeline>();
        await handle(pipeline, envelope, ct);

        return AckStrategies.AckAsync;
    }
}
