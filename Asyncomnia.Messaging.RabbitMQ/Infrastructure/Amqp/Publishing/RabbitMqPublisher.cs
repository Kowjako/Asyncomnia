using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;

internal interface IRabbitMqPublisher
{
    Task PublishAsync(string exchange, string routingKey, bool mandatory, BasicProperties properties,
        ReadOnlyMemory<byte> body, CancellationToken token = default);
}

internal sealed class RabbitMqPublisher(IRabbitMqChannelPool channelPool) : IRabbitMqPublisher
{
    private readonly IRabbitMqChannelPool _channelPool = channelPool
        ?? throw new ArgumentNullException(nameof(channelPool));

    public async Task PublishAsync(string exchange, string routingKey, bool mandatory, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(exchange);
        ArgumentNullException.ThrowIfNull(routingKey);
        ArgumentNullException.ThrowIfNull(properties);

        var slot = await _channelPool.RentAsync(token);

        try
        {
            await slot.PublishAsync(exchange, routingKey, mandatory, properties, body, token);
        }
        finally
        {
            _channelPool.Return(slot);
        }
    }
}