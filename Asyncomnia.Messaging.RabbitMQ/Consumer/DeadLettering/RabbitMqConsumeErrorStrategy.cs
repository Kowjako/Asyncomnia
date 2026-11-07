using Asyncomnia.Messaging.RabbitMQ.Configuration;
using EasyNetQ.Consumer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;

internal sealed class RabbitMqConsumeErrorStrategy(IRabbitMqDeadLetterPublisher deadLetterPublisher, IOptions<RabbitMqConsumerOptions> options,
    ILogger<RabbitMqConsumeErrorStrategy> logger) : IConsumeErrorStrategy
{
    private readonly IRabbitMqDeadLetterPublisher _deadLetterPublisher = deadLetterPublisher
        ?? throw new ArgumentNullException(nameof(deadLetterPublisher));

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

        if (!_options.DeadLetteringEnabled)
        {
            return AckStrategies.NackWithoutRequeueAsync;
        }

        try
        {
            await _deadLetterPublisher.PublishAsync(context, token);

            _logger.LogWarning("Message from queue {QueueName} was published to the dead-letter exchange", context.ReceivedInfo.Queue);

            return AckStrategies.AckAsync;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            _logger.LogDebug("Dead-letter publishing was cancelled for queue {QueueName}. Message will be requeued", context.ReceivedInfo.Queue);

            return AckStrategies.NackWithRequeueAsync;
        }
        catch (Exception publishException)
        {
            _logger.LogError(
                publishException,
                "Failed to publish message from queue {QueueName} to the dead-letter exchange. Message will be requeued",
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