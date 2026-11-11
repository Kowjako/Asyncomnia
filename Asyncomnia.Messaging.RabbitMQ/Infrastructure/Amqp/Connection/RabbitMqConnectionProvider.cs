using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Connection;

internal interface IRabbitMqConnectionProvider
{
    ValueTask<IConnection> GetConnectionAsync(CancellationToken token = default);
}

internal sealed class RabbitMqConnectionProvider(IOptions<RabbitMqOptions> options)
    : IRabbitMqConnectionProvider, IDisposable, IAsyncDisposable
{
    private readonly RabbitMqOptions _options = options?.Value
        ?? throw new ArgumentNullException(nameof(options));

    // Multiple components may request channels concurrently during startup.
    // All channels must share one RabbitMQ connection, so this semaphore
    // serializes initialization and prevents concurrent callers from creating
    // separate connections. The same lock also coordinates initialization
    // with provider shutdown.
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    private IConnection? _connection;
    private bool _disposed;

    public async ValueTask<IConnection> GetConnectionAsync(CancellationToken token = default)
    {
        await _connectionLock.WaitAsync(token);

        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_connection is not null)
            {
                return _connection;
            }

            var factory = new ConnectionFactory
            {
                Uri = new Uri(_options.ConnectionString, UriKind.Absolute),
                // Connection and topology recovery are delegated to RabbitMQ.Client.
                // Asyncomnia should not create a replacement connection during recovery.
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
            };

            _connection = await factory.CreateConnectionAsync(token);

            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _connectionLock.WaitAsync().ConfigureAwait(false);

        try
        {
            if (!TryBeginDispose(out var connection))
            {
                return;
            }

            if (connection is not null)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    public void Dispose()
    {
        _connectionLock.Wait();

        try
        {
            if (!TryBeginDispose(out var connection))
            {
                return;
            }

            connection?.Dispose();
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private bool TryBeginDispose(out IConnection? connection)
    {
        if (_disposed)
        {
            connection = null;
            return false;
        }

        _disposed = true;

        connection = _connection;
        _connection = null;

        return true;
    }
}
