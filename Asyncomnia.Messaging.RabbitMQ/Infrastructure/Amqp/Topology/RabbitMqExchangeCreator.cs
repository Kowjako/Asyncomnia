using System.Collections.Concurrent;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology.Arguments;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;

internal interface IRabbitMqExchangeCreator
{
    Task EnsureDeclaredAsync(
        string exchangeName,
        bool useUnroutableFallback,
        CancellationToken token = default);
}

internal sealed class RabbitMqExchangeCreator(IRabbitMqChannelFactory channelFactory,
    IRabbitMqUnroutableTopologyCreator unroutableTopologyCreator) : IRabbitMqExchangeCreator
{
    private readonly IRabbitMqChannelFactory _channelFactory = channelFactory
        ?? throw new ArgumentNullException(nameof(channelFactory));

    private readonly IRabbitMqUnroutableTopologyCreator _unroutableTopologyCreator = unroutableTopologyCreator
        ?? throw new ArgumentNullException(nameof(unroutableTopologyCreator));

    private readonly ConcurrentDictionary<string, bool> _declaredExchanges = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, SemaphoreSlim> _declarationLocks = new(StringComparer.Ordinal);

    public async Task EnsureDeclaredAsync(string exchangeName, bool useUnroutableFallback, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(exchangeName);

        if (CacheContainsExchange(exchangeName, useUnroutableFallback))
        {
            return;
        }

        var declarationLock = _declarationLocks.GetOrAdd(exchangeName, _ => new SemaphoreSlim(1, 1));
        await declarationLock.WaitAsync(token);

        try
        {
            if (CacheContainsExchange(exchangeName, useUnroutableFallback))
            {
                return;
            }

            await DeclareCoreAsync(exchangeName, useUnroutableFallback, token);
            _declaredExchanges.TryAdd(exchangeName, useUnroutableFallback);
        }
        finally
        {
            declarationLock.Release();
        }
    }

    private bool CacheContainsExchange(string exchangeName, bool useUnroutableFallback)
    {
        if (!_declaredExchanges.TryGetValue(exchangeName, out var declaredWithFallback))
        {
            return false;
        }

        if (declaredWithFallback != useUnroutableFallback)
        {
            throw new InvalidOperationException($"RabbitMQ exchange '{exchangeName}' was already declared with a different unroutable fallback configuration.");
        }

        return true;
    }

    private async Task DeclareCoreAsync(string exchangeName, bool useUnroutableFallback, CancellationToken token)
    {
        if (useUnroutableFallback)
        {
            await _unroutableTopologyCreator.EnsureDeclaredAsync(token);
        }

        await using var channel = await _channelFactory.CreateAsync(token: token);

        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            arguments: RabbitMqExchangeArgumentsFactory.Create(useUnroutableFallback),
            passive: false,
            noWait: false,
            cancellationToken: token);
    }
}
