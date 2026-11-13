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
        var context = new RabbitMqConsumeContext(
            subscription.QueueName,
            delivery.Exchange,
            delivery.RoutingKey,
            delivery.BasicProperties,
            delivery.Body);

        var response = AckResponse.Ack;

        try
        {
            await ProcessAsync(delivery, subscription, token);
            response = AckResponse.Ack;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            response = AckResponse.NackWithRequeue;
        }
        catch (Exception ex)
        {
            response = await _errorStrategy.HandleErrorAsync(context, ex, token);
        }

        // ACK/NACK after try-catch, so if message will be handled correctly, but BasicAck will fail
        // we will not publish message to DLQ
        await ReturnResponseToBrokerAsync(channel, delivery.DeliveryTag, response, CancellationToken.None);
    }

    private async Task ProcessAsync(BasicDeliverEventArgs delivery, RabbitMqSubscription subscription, CancellationToken token)
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

        if (!subscription.Bindings.Any(x => x.MessageType == descriptor.MessageType))
        {
            throw new InvalidOperationException($"Message type '{descriptor.MessageType.FullName}' is not configured for subscription queue '{subscription.QueueName}'");
        }

        var metadata = RabbitMqMessagePropertiesMapper.MapToAsyncomniaProperties(delivery.BasicProperties);
        var message = _messageSerializer.Deserialize(delivery.Body, descriptor.MessageType);

        var context = new InboundMessageContext(subscription.QueueName, contractType, message, metadata);

        await using var scope = _scopeFactory.CreateAsyncScope();

        var pipeline = scope.ServiceProvider.GetRequiredService<IInboundMessagePipeline>();
        await descriptor.DispatchAsync(pipeline, context, token);
    }

    private static ValueTask ReturnResponseToBrokerAsync(IChannel channel, ulong deliveryTag, AckResponse response, CancellationToken token)
    {
        return response switch
        {
            AckResponse.Ack => channel.BasicAckAsync(deliveryTag, multiple: false, token),
            AckResponse.NackWithRequeue => channel.BasicNackAsync(deliveryTag, multiple: false, requeue: true, token),
            AckResponse.NackWithoutRequeue => channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false, token),
            _ => throw new ArgumentOutOfRangeException(nameof(response))
        };
    }
}