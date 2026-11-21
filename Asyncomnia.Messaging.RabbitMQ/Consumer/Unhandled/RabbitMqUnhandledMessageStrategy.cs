using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.Unhandled;

internal sealed class RabbitMqUnhandledMessageStrategy(
    ILogger<RabbitMqUnhandledMessageStrategy> logger,
    RabbitMqUnhandledRouteResolver routeResolver,
    IRabbitMqPublisher rabbitMqPublisher)
{
    private const string ReasonHeader = "x-asyncomnia-unhandled-reason";
    private const string SourceQueueHeader = "x-asyncomnia-source-queue";
    private const string SourceExchangeHeader = "x-asyncomnia-source-exchange";
    private const string SourceRoutingKeyHeader = "x-asyncomnia-source-routing-key";

    public async ValueTask<AckResponse> HandleAsync(RabbitMqConsumeContext context, string reason, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);

        try
        {
            var route = routeResolver.Resolve(context.QueueName);
            var properties = WithReason(context, reason);

            await rabbitMqPublisher.PublishAsync(
                exchange: route.ExchangeName,
                routingKey: route.RoutingKey,
                mandatory: true,
                properties: properties,
                body: context.Body,
                token: token);

            logger.LogWarning(
                "RabbitMQ message was moved to unhandled queue. " +
                "SourceQueue={QueueName}, ContractType={ContractType}, Reason={Reason}",
                context.QueueName,
                context.Properties.Type,
                reason);

            return AckResponse.Ack;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            return AckResponse.NackWithRequeue;
        }
        catch (Exception exception)
        {
            logger.LogError(
                exception,
                "Failed to move RabbitMQ message to unhandled queue. " +
                "SourceQueue={QueueName}, ContractType={ContractType}",
                context.QueueName,
                context.Properties.Type);

            return AckResponse.NackWithRequeue;
        }
    }

    private static BasicProperties WithReason(RabbitMqConsumeContext context, string reason)
    {
        var headers = context.Properties.Headers is null
            ? new Dictionary<string, object?>()
            : new Dictionary<string, object?>(context.Properties.Headers);

        headers[ReasonHeader] = reason;
        headers[SourceQueueHeader] = context.QueueName;
        headers[SourceExchangeHeader] = context.ExchangeName;
        headers[SourceRoutingKeyHeader] = context.RoutingKey;

        return new BasicProperties(context.Properties)
        {
            Headers = headers,
            Expiration = null
        };
    }
}