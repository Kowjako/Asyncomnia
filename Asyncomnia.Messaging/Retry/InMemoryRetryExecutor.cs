using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Retry;

internal interface IInMemoryRetryExecutor
{
    Task ExecuteAsync<TMessage>(MessageEnvelope<TMessage> envelope, Type handlerType, Func<CancellationToken, Task> operation, CancellationToken token = default)
        where TMessage : class, IMessage;
}

internal sealed class InMemoryRetryExecutor(IOptions<InMemoryRetryOptions> options, TimeProvider timeProvider, ILogger<InMemoryRetryExecutor>? logger = null) : IInMemoryRetryExecutor
{
    private readonly IReadOnlyList<TimeSpan> _delays = options?.Value.Delays
        ?? throw new ArgumentNullException(nameof(options));

    private readonly TimeProvider _timeProvider = timeProvider
        ?? throw new ArgumentNullException(nameof(timeProvider));

    private readonly ILogger<InMemoryRetryExecutor> _logger = logger ?? NullLogger<InMemoryRetryExecutor>.Instance;

    public async Task ExecuteAsync<TMessage>(MessageEnvelope<TMessage> envelope, Type handlerType, Func<CancellationToken, Task> operation, CancellationToken token = default)
        where TMessage : class, IMessage
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(handlerType);
        ArgumentNullException.ThrowIfNull(operation);

        for (var attempt = 0; ; attempt++)
        {
            token.ThrowIfCancellationRequested();

            try
            {
                await operation(token);
                return;
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception) when (attempt < _delays.Count)
            {
                var retryNumber = attempt + 1;
                var delay = _delays[attempt];

                _logger.LogWarning(exception,
                    "Handler {HandlerType} failed while processing message {MessageType} with ID {MessageId}. In-memory retry {RetryNumber}/{RetryCount} will run after {RetryDelay}",
                    handlerType.FullName,
                    typeof(TMessage).FullName,
                    envelope.Metadata.MessageId,
                    retryNumber,
                    _delays.Count,
                    delay);

                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, _timeProvider, token);
                }
            }
        }
    }
}