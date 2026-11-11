using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;

internal interface IRabbitMqDeadLetterPublisher
{
    Task PublishAsync(RabbitMqConsumeContext context, CancellationToken token = default);
}

internal sealed class RabbitMqDeadLetterPublisher(IRabbitMqPublisher rabbitMqPublisher, RabbitMqDeadLetterRouteResolver routeResolver) 
    : IRabbitMqDeadLetterPublisher
{
    private readonly IRabbitMqPublisher _rabbitMqPublisher = rabbitMqPublisher
        ?? throw new ArgumentNullException(nameof(rabbitMqPublisher));

    private readonly RabbitMqDeadLetterRouteResolver _routeResolver = routeResolver
        ?? throw new ArgumentNullException(nameof(routeResolver));

    public async Task PublishAsync(RabbitMqConsumeContext context, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        token.ThrowIfCancellationRequested();

        var route = _routeResolver.Resolve(context.QueueName);

        var properties = new BasicProperties(context.Properties)
        {
            Expiration = null // remove per-message TTL so after publication to DLQ it will not disappear :D 
        };

        await _rabbitMqPublisher.PublishAsync(
            exchange: route.ExchangeName,
            routingKey: route.RoutingKey,
            mandatory: true,
            properties: properties,
            body: context.Body,
            token);
    }
}
