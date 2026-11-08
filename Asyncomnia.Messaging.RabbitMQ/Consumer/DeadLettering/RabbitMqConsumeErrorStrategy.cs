using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using EasyNetQ.Consumer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;

internal sealed class RabbitMqConsumeErrorStrategy(
    IRabbitMqDeadLetterPublisher deadLetterPublisher,
    IRabbitMqDelayedRetryPublisher delayedRetryPublisher,
    IOptions<RabbitMqConsumerOptions> options,
    ILogger<RabbitMqConsumeErrorStrategy> logger) : IConsumeErrorStrategy
{
    private readonly IRabbitMqDeadLetterPublisher _deadLetterPublisher = deadLetterPublisher
        ?? throw new ArgumentNullException(nameof(deadLetterPublisher));

    private readonly IRabbitMqDelayedRetryPublisher _delayedRetryPublisher = delayedRetryPublisher
        ?? throw new ArgumentNullException(nameof(delayedRetryPublisher));

    private readonly RabbitMqConsumerOptions _options = options?.Value
        ?? throw new ArgumentNullException(nameof(options));

    private readonly ILogger<RabbitMqConsumeErrorStrategy> _logger = logger
        ?? throw new ArgumentNullException(nameof(logger));

    public async ValueTask<AckStrategyAsync> HandleErrorAsync(ConsumeContext context, Exception exception, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(exception);

        _logger.LogError(
            exception,
            "RabbitMQ message processing failed. Queue={QueueName}, Exchange={ExchangeName}, RoutingKey={RoutingKey}",
            context.ReceivedInfo.Queue,
            context.ReceivedInfo.Exchange,
            context.ReceivedInfo.RoutingKey);

        try
        {
            if (await _delayedRetryPublisher.TryPublishAsync(context, token))
            {
                _logger.LogWarning("Message from queue {QueueName} was scheduled for broker retry", context.ReceivedInfo.Queue);
                return AckStrategies.AckAsync;
            }

            if (!_options.DeadLetteringEnabled)
            {
                return AckStrategies.NackWithoutRequeueAsync;
            }

            await _deadLetterPublisher.PublishAsync(context, token);

            _logger.LogWarning("Message from queue {QueueName} was published to the dead-letter exchange", context.ReceivedInfo.Queue);

            return AckStrategies.AckAsync;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            _logger.LogDebug("Retry or dead-letter publishing was cancelled for queue {QueueName}. Message will be requeued", context.ReceivedInfo.Queue);

            return AckStrategies.NackWithRequeueAsync;
        }
        catch (Exception publishException)
        {
            _logger.LogError(
                publishException,
                "Failed to publish message from queue {QueueName} to retry or dead-letter topology. Message will be requeued",
                context.ReceivedInfo.Queue);

            return AckStrategies.NackWithRequeueAsync;
        }
    }

    public ValueTask<AckStrategyAsync> HandleCancelledAsync(ConsumeContext context, CancellationToken token = default)
    {
        _logger.LogDebug(
            "RabbitMQ message processing was cancelled for queue {QueueName}. Message will be requeued",
            context.ReceivedInfo.Queue);

        return new ValueTask<AckStrategyAsync>(AckStrategies.NackWithRequeueAsync);
    }
}
