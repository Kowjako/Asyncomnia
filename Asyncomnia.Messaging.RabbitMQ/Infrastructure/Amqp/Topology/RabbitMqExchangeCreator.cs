using System.Collections.Concurrent;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;

internal interface IRabbitMqExchangeCreator
{
    Task EnsureDeclaredAsync(string exchangeName, CancellationToken token = default);
}

internal sealed class RabbitMqExchangeCreator(IRabbitMqChannelFactory channelFactory) : IRabbitMqExchangeCreator
{
    private readonly IRabbitMqChannelFactory _channelFactory = channelFactory
        ?? throw new ArgumentNullException(nameof(channelFactory));

    private readonly ConcurrentDictionary<string, byte> _declaredExchanges = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, SemaphoreSlim> _declarationLocks = new(StringComparer.Ordinal);

    public async Task EnsureDeclaredAsync(string exchangeName, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(exchangeName);

        if (_declaredExchanges.ContainsKey(exchangeName))
        {
            return;
        }

        var declarationLock = _declarationLocks.GetOrAdd(exchangeName, _ => new SemaphoreSlim(1, 1));
        await declarationLock.WaitAsync(token);

        try
        {
            if (_declaredExchanges.ContainsKey(exchangeName))
            {
                return;
            }

            await DeclareCoreAsync(exchangeName, token);
            _declaredExchanges.TryAdd(exchangeName, 0);
        }
        finally
        {
            declarationLock.Release();
        }
    }

    private async Task DeclareCoreAsync(string exchangeName, CancellationToken token)
    {
        await using var channel = await _channelFactory.CreateAsync(token: token);

        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            arguments: null,
            passive: false,
            noWait: false,
            cancellationToken: token);
    }
}