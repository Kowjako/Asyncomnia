using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Internal.Serialization;
using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Serialization;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal sealed class RabbitMqMessageConsumer(
    IServiceScopeFactory scopeFactory,
    MessageJsonSerializer messageSerializer,
    IMessageDispatchCatalog dispatchCatalog,
    RabbitMqConsumeErrorStrategy errorStrategy) 
{
    private readonly IServiceScopeFactory _scopeFactory = scopeFactory
        ?? throw new ArgumentNullException(nameof(scopeFactory));

    private readonly MessageJsonSerializer _messageSerializer = messageSerializer
        ?? throw new ArgumentNullException(nameof(messageSerializer));

    private readonly IMessageDispatchCatalog _dispatchCatalog = dispatchCatalog
        ?? throw new ArgumentNullException(nameof(dispatchCatalog));

    private readonly RabbitMqConsumeErrorStrategy _errorStrategy = errorStrategy
        ?? throw new ArgumentNullException(nameof(errorStrategy));

    public async Task ConsumeAsync(IChannel channel, RabbitMqSubscription subscription, BasicDeliverEventArgs delivery, CancellationToken token)
    {
        try
        {
            await ProcessAsync(delivery, token);

            await channel.BasicAckAsync(
                deliveryTag: delivery.DeliveryTag,
                multiple: false,
                cancellationToken: token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            await channel.BasicNackAsync(
                deliveryTag: delivery.DeliveryTag,
                multiple: false,
                requeue: true,
                cancellationToken: CancellationToken.None);
        }
        catch (Exception ex)
        {

        }
    }

    private async Task ProcessAsync(BasicDeliverEventArgs delivery, CancellationToken token)
    {
        var contractType = delivery.BasicProperties.Type;

        if (string.IsNullOrWhiteSpace(contractType))
        {
            throw new InvalidOperationException("RabbitMQ message does not define a contract type");
        }

        if (!_dispatchCatalog.TryGet(contractType, out var descriptor) || descriptor is null)
        {
            throw new InvalidOperationException($"No dispatch descriptor registered for contract {contractType}");
        }

        var metadata = RabbitMqMessagePropertiesMapper.MapToAsyncomniaProperties(delivery.BasicProperties);
        var message = _messageSerializer.Deserialize(delivery.Body, descriptor.MessageType);

        await using var scope = _scopeFactory.CreateAsyncScope();

        var pipeline = scope.ServiceProvider.GetRequiredService<IInboundMessagePipeline>();
        await descriptor.DispatchAsync(pipeline, message, metadata, token);
    }
}
