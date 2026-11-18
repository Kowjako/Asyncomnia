using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;

internal interface IRabbitMqUnroutableTopologyCreator
{
    Task EnsureDeclaredAsync(CancellationToken token = default);
}

internal sealed class RabbitMqUnroutableTopologyCreator(
    IRabbitMqChannelFactory channelFactory, IOptions<RabbitMqOptions> rabbitMqOptions) : IRabbitMqUnroutableTopologyCreator
{
    private readonly IRabbitMqChannelFactory _channelFactory = channelFactory
        ?? throw new ArgumentNullException(nameof(channelFactory));

    private readonly RabbitMqOptions _rabbitMqOptions = rabbitMqOptions?.Value
        ?? throw new ArgumentNullException(nameof(rabbitMqOptions));

    private readonly SemaphoreSlim _declarationLock = new(1, 1);
    private bool _declared;

    public async Task EnsureDeclaredAsync(CancellationToken token = default)
    {
        await _declarationLock.WaitAsync(token);

        try
        {
            if (_declared)
            {
                return;
            }

            await using var channel = await _channelFactory.CreateAsync(token: token);

            await channel.ExchangeDeclareAsync(
                exchange: RabbitMqUnroutableDefaults.ExchangeName,
                type: ExchangeType.Fanout,
                durable: true,
                autoDelete: false,
                arguments: null,
                passive: false,
                noWait: false,
                cancellationToken: token);

            await channel.QueueDeclareAsync(
                queue: RabbitMqUnroutableDefaults.QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: RabbitMqUnroutableQueueArgumentsFactory.Create(_rabbitMqOptions),
                passive: false,
                noWait: false,
                cancellationToken: token);

            await channel.QueueBindAsync(
                queue: RabbitMqUnroutableDefaults.QueueName,
                exchange: RabbitMqUnroutableDefaults.ExchangeName,
                routingKey: string.Empty,
                arguments: null,
                noWait: false,
                cancellationToken: token);

            _declared = true;
        }
        finally
        {
            _declarationLock.Release();
        }
    }
}
