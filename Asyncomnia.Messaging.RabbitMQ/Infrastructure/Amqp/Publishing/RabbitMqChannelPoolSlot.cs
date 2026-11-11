using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;

internal sealed class RabbitMqChannelPoolSlot : IDisposable, IAsyncDisposable
{
    private static readonly CreateChannelOptions ChannelOptions = new(
        publisherConfirmationsEnabled: true,
        publisherConfirmationTrackingEnabled: true);

    private readonly IRabbitMqChannelFactory _channelFactory;

    // Pool guarantees exclusive slot leasing, while this lock additionally
    // coordinates an active publication with application shutdown.
    private readonly SemaphoreSlim _channelLock = new(1, 1);

    private IChannel? _channel;
    private bool _disposed;

    public RabbitMqChannelPoolSlot(IRabbitMqChannelFactory channelFactory)
    {
        _channelFactory = channelFactory ?? throw new ArgumentNullException(nameof(channelFactory));
    }

    public async Task PublishAsync(string exchange, string routingKey, bool mandatory, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken token)
    {
        await _channelLock.WaitAsync(token);

        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var channel = await GetOrCreateChannelAsync(token);

            await channel.BasicPublishAsync(
                exchange: exchange,
                routingKey: routingKey,
                mandatory: mandatory,
                basicProperties: properties,
                body: body,
                cancellationToken: token);
        }
        finally
        {
            _channelLock.Release();
        }
    }

    private async Task<IChannel> GetOrCreateChannelAsync(CancellationToken token)
    {
        // Self-healing. RabbitMQ.AutomaticRecoveryEnabled is tracking connection itself,
        // if connection (TCP) is still available, but someting happens with the channel itself - it will not 
        // be automatically re-created, so dispose it and create a new.
        if (_channel is { IsOpen: true })
        {
            return _channel;
        }

        if (_channel is not null)
        {
            await _channel.DisposeAsync();
            _channel = null;
        }

        _channel = await _channelFactory.CreateAsync(ChannelOptions, token);

        return _channel;
    }

    public async ValueTask DisposeAsync()
    {
        await _channelLock.WaitAsync().ConfigureAwait(false);

        try
        {
            if (!TryBeginDispose(out var channel))
            {
                return;
            }

            if (channel is not null)
            {
                await channel.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _channelLock.Release();
        }
    }

    public void Dispose()
    {
        _channelLock.Wait();

        try
        {
            if (!TryBeginDispose(out var channel))
            {
                return;
            }

            channel?.Dispose();
        }
        finally
        {
            _channelLock.Release();
        }
    }

    private bool TryBeginDispose(out IChannel? channel)
    {
        if (_disposed)
        {
            channel = null;
            return false;
        }

        _disposed = true;

        channel = _channel;
        _channel = null;

        return true;
    }
}
