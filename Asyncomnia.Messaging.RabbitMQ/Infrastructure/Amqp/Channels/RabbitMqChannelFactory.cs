using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Connection;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;

internal interface IRabbitMqChannelFactory
{
    Task<IChannel> CreateAsync(CreateChannelOptions? options = null, CancellationToken token = default);
}

internal sealed class RabbitMqChannelFactory(IRabbitMqConnectionProvider connectionProvider) : IRabbitMqChannelFactory
{
    private readonly IRabbitMqConnectionProvider _connectionProvider = connectionProvider
        ?? throw new ArgumentNullException(nameof(connectionProvider));

    public async Task<IChannel> CreateAsync(CreateChannelOptions? options = null, CancellationToken token = default)
    {
        var connection = await _connectionProvider.GetConnectionAsync(token);
        return await connection.CreateChannelAsync(options, token);
    }
}