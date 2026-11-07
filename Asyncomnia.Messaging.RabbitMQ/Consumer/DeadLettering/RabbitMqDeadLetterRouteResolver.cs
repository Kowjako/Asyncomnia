namespace Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;

internal sealed class RabbitMqDeadLetterRouteResolver
{
    private const string ExchangeName = "asyncomnia/deadletter";
    private const string QueuePrefix = "deadletter/";

    public RabbitMqDeadLetterRoute Resolve(string sourceQueueName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceQueueName);

        var deadLetterQueueName = $"{QueuePrefix}{sourceQueueName}";

        return new RabbitMqDeadLetterRoute(ExchangeName, deadLetterQueueName, deadLetterQueueName);
    }
}