using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal interface IRabbitMqQueueConsumerFactory
{
    Task<IAsyncDisposable> StartAsync(RabbitMqSubscription subscription, CancellationToken token = default);
}

internal sealed class RabbitMqQueueConsumerFactory(IRabbitMqChannelFactory channelFactory,
    RabbitMqMessageConsumer messageConsumer) : IRabbitMqQueueConsumerFactory
{
    private readonly IRabbitMqChannelFactory _channelFactory = channelFactory
        ?? throw new ArgumentNullException(nameof(channelFactory));

    private readonly RabbitMqMessageConsumer _messageConsumer = messageConsumer
        ?? throw new ArgumentNullException(nameof(messageConsumer));

    public async Task<IAsyncDisposable> StartAsync(RabbitMqSubscription subscription, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(subscription);
        token.ThrowIfCancellationRequested();

        var channel = await _channelFactory.CreateAsync(CreateConsumerChannelOptions(subscription), token);

        try
        {
            await channel.BasicQosAsync(
                prefetchSize: 0, // 0 means ignore prefetch total size
                prefetchCount: subscription.ConsumerSettings.PrefetchCount,
                global: false, // false - means this limit only for this consumer
                cancellationToken: token);

            var consumer = new AsyncEventingBasicConsumer(channel);

            consumer.ReceivedAsync += (_, delivery) =>
                _messageConsumer.ConsumeAsync(channel, subscription, delivery, delivery.CancellationToken);
            
            await channel.BasicConsumeAsync(
                queue: subscription.QueueName,
                autoAck: false,
                consumer,
                cancellationToken: token);

            return channel;
        }
        catch
        {
            await channel.DisposeAsync();
            throw;
        }
    }

    private CreateChannelOptions CreateConsumerChannelOptions(RabbitMqSubscription subscription)
    {
        // consumer dont need publisher confirms, since consumer only listening
        return new CreateChannelOptions(
            publisherConfirmationsEnabled: false,
            publisherConfirmationTrackingEnabled: false,
            outstandingPublisherConfirmationsRateLimiter: null,
            consumerDispatchConcurrency: subscription.ConsumerSettings.DispatcherConcurrency);
    }
}