using Asyncomnia.Messaging.Internal;
using Asyncomnia.Messaging.Internal.Serialization;
using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Asyncomnia.Messaging.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Outbox.Processing;

internal sealed class OutboxBatchProcessor
{
    private readonly IOutboxStore _outboxStore;
    private readonly IOutboundTransport _transport;
    private readonly MessageJsonSerializer _serializer;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxBatchProcessor> _logger;

    public OutboxBatchProcessor(
        IOutboxStore outboxStore,
        IOutboundTransport transport,
        MessageJsonSerializer serializer,
        IOptions<OutboxOptions> options,
        ILogger<OutboxBatchProcessor> logger)
    {
        _outboxStore = outboxStore;
        _transport = transport;
        _serializer = serializer;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<int> ProcessBatchAsync(CancellationToken token = default)
    {
        var messages = await _outboxStore.ClaimAsync(_options.BatchSize, _options.LeaseDuration, token);
        foreach (var message in messages)
        {
            await ProcessMessageAsync(message, token);
        }

        return messages.Count;
    }

    private async Task ProcessMessageAsync(ClaimedOutboxMessage claimedMessage, CancellationToken token = default)
    {
        if (claimedMessage.AttemptNumber > _options.MaxAttempts)
        {
            var error = $"Maximum number of attempts exceeded. Attempt: {claimedMessage.AttemptNumber}, maximum: {_options.MaxAttempts}.";

            _logger.LogError(
                "Outbox message {MessageId} was claimed with attempt {AttemptNumber} above the configured maximum {MaxAttempts}",
                claimedMessage.Message.MessageId,
                claimedMessage.AttemptNumber,
                _options.MaxAttempts);

            await FailAsync(claimedMessage, error, token);
            return;
        }

        try
        {
            var clrType = ClrMessageTypeResolver.Resolve(claimedMessage.Message.ContractType);
            var message = _serializer.Deserialize(claimedMessage.Message.MessageJson, clrType);

            var metadata = new MessageMetadata(claimedMessage.Message.MessageId, claimedMessage.Message.CreatedAt)
            {
                CorrelationId = claimedMessage.Message.CorrelationId,
            };

            await _transport.DispatchAsync(message, metadata, token);

            var completed = await _outboxStore.CompleteAsync(metadata.MessageId, claimedMessage.LeaseId, token);
            if (!completed)
            {
                LogLeaseLost("complete", claimedMessage);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            if (claimedMessage.AttemptNumber >= _options.MaxAttempts)
            {
                _logger.LogError(ex,
                    "Outbox message {MessageId} failed on its final attempt {AttemptNumber}",
                    claimedMessage.Message.MessageId,
                    claimedMessage.AttemptNumber);

                await FailAsync(claimedMessage, ex.Message, token);
            }
            else
            {
                _logger.LogWarning(ex,
                    "Outbox message {MessageId} failed on attempt {AttemptNumber}; scheduling retry after {RetryDelay}",
                    claimedMessage.Message.MessageId,
                    claimedMessage.AttemptNumber,
                    _options.RetryDelay);

                var retryScheduled = await _outboxStore.ScheduleRetryAsync(claimedMessage.Message.MessageId,
                    claimedMessage.LeaseId, ex.Message, _options.RetryDelay, token);

                if (!retryScheduled)
                {
                    LogLeaseLost("schedule retry for", claimedMessage);
                }
            }
        }
    }

    private async Task FailAsync(ClaimedOutboxMessage claimedMessage, string error, CancellationToken token)
    {
        var failed = await _outboxStore.FailAsync(claimedMessage.Message.MessageId, claimedMessage.LeaseId, error, token);

        if (!failed)
        {
            LogLeaseLost("mark as failed", claimedMessage);
        }
    }

    private void LogLeaseLost(string operation, ClaimedOutboxMessage claimedMessage)
    {
        _logger.LogWarning(
            "Could not {Operation} outbox message {MessageId} because lease {LeaseId} is no longer owned",
            operation,
            claimedMessage.Message.MessageId,
            claimedMessage.LeaseId);
    }
}
