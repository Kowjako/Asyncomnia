using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;

internal sealed class RabbitMqConsumeErrorStrategy(
    IRabbitMqDeadLetterPublisher deadLetterPublisher,
    IRabbitMqDelayedRetryPublisher delayedRetryPublisher,
    IOptions<RabbitMqConsumerOptions> options,
    ILogger<RabbitMqConsumeErrorStrategy> logger)
{
    private readonly IRabbitMqDeadLetterPublisher _deadLetterPublisher = deadLetterPublisher
        ?? throw new ArgumentNullException(nameof(deadLetterPublisher));

    private readonly IRabbitMqDelayedRetryPublisher _delayedRetryPublisher = delayedRetryPublisher
        ?? throw new ArgumentNullException(nameof(delayedRetryPublisher));

    private readonly RabbitMqConsumerOptions _options = options?.Value
        ?? throw new ArgumentNullException(nameof(options));

    private readonly ILogger<RabbitMqConsumeErrorStrategy> _logger = logger
        ?? throw new ArgumentNullException(nameof(logger));

    public async ValueTask<AckResponse> HandleErrorAsync(RabbitMqConsumeContext context, Exception exception, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(exception);

        _logger.LogError(
            exception,
            "RabbitMQ message processing failed. Queue={QueueName}, Exchange={ExchangeName}, RoutingKey={RoutingKey}",
            context.QueueName,
            context.ExchangeName,
            context.RoutingKey);

        try
        {
            if (await _delayedRetryPublisher.TryPublishAsync(context, token))
            {
                _logger.LogWarning("Message from queue {QueueName} was scheduled for broker retry", context.QueueName);
                return AckResponse.Ack;
            }

            if (!_options.DeadLetteringEnabled)
            {
                return AckResponse.NackWithoutRequeue;
            }

            await _deadLetterPublisher.PublishAsync(context, token);

            _logger.LogWarning("Message from queue {QueueName} was published to the dead-letter exchange", context.QueueName);

            return AckResponse.Ack;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            _logger.LogDebug("Retry or dead-letter publishing was cancelled for queue {QueueName}. Message will be requeued", context.QueueName);

            return AckResponse.NackWithoutRequeue;
        }
        catch (Exception publishException)
        {
            _logger.LogError(
                publishException,
                "Failed to publish message from queue {QueueName} to retry or dead-letter topology. Message will be requeued",
                context.QueueName);

            return AckResponse.NackWithRequeue;
        }
    }
}
