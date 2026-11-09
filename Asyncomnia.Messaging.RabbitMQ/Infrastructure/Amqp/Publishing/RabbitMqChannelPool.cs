using System.Threading.Channels;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;

internal interface IRabbitMqChannelPool : IAsyncDisposable
{
    ValueTask<RabbitMqChannelPoolSlot> RentAsync(CancellationToken token = default);

    void Return(RabbitMqChannelPoolSlot slot);
}

internal sealed class RabbitMqChannelPool : IRabbitMqChannelPool
{
    private readonly Channel<RabbitMqChannelPoolSlot> _availableSlots;
    private readonly RabbitMqChannelPoolSlot[] _slots;

    // Use int, to operate using Interlocked (0 = false, 1 = true)
    private int _disposed;

    public RabbitMqChannelPool(IRabbitMqChannelFactory channelFactory, IOptions<RabbitMqOptions> options)
    {
        ArgumentNullException.ThrowIfNull(channelFactory);

        var publisherChannelCount = options?.Value.PublisherChannelCount ?? throw new ArgumentNullException(nameof(options));

        _availableSlots = Channel.CreateBounded<RabbitMqChannelPoolSlot>(publisherChannelCount);

        _slots = Enumerable.Range(0, publisherChannelCount)
            .Select(_ => new RabbitMqChannelPoolSlot(channelFactory))
            .ToArray();

        foreach (var slot in _slots)
        {
            if (!_availableSlots.Writer.TryWrite(slot))
            {
                throw new InvalidOperationException("Could not initialize RabbitMQ publisher channel pool");
            }
        }
    }

    public async ValueTask<RabbitMqChannelPoolSlot> RentAsync(CancellationToken token = default)
    {
        ThrowIfDisposed();

        try
        {
            var slot = await _availableSlots.Reader.ReadAsync(token);

            ThrowIfDisposed();

            return slot;
        }
        catch (ChannelClosedException) when (IsDisposed())
        {
            throw new ObjectDisposedException(nameof(RabbitMqChannelPool));
        }
    }

    public void Return(RabbitMqChannelPoolSlot slot)
    {
        ArgumentNullException.ThrowIfNull(slot);

        if (IsDisposed())
        {
            return;
        }

        _availableSlots.Writer.TryWrite(slot);
    }

    public async ValueTask DisposeAsync()
    {
        // Change state from 0 to 1 atomically. If we did this as first, then close writer
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _availableSlots.Writer.TryComplete();

        await Task.WhenAll(_slots.Select(slot => slot.DisposeAsync().AsTask()));
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(IsDisposed(), this);
    }

    private bool IsDisposed()
    {
        // Reading with memory guarantee, without blocking a thread
        return Volatile.Read(ref _disposed) != 0;
    }
}
