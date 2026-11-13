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

        // Single channel per single consumer,
        // running multiple consumers over single channel its antipattern.
        // RabbitMQ Channel is not thread-safe, so if multiple consumers (AsyncEventingBasicConsumer for example)
        // will try at the same time send Ack/Nack it can end up with I/O Exception or TCP buffet exceptions
        // Also during consuming message if we want to publish we need to do this over new channel
        // (or rented from publisher channel pool). It means we cannot consume and publish via the same channel
        // if we want safe code and avoid IO/TCP Exceptions.
        // Gold statement: 1 consumer / 1 listening loop = 1 separate Channel
        var channel = await _channelFactory.CreateAsync(CreateConsumerChannelOptions(subscription), token);

        try
        {
            await channel.BasicQosAsync(
                prefetchSize: 0, // 0 means ignore prefetch total size
                prefetchCount: subscription.Settings.PrefetchCount,
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
            consumerDispatchConcurrency: subscription.Settings.DispatcherConcurrency);
    }
}
