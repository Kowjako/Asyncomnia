namespace Asyncomnia.Messaging.RabbitMQ.Consumer.Unhandled;

internal sealed class RabbitMqUnhandledRouteResolver
{
    private const string ExchangeName = "asyncomnia/unhandled";
    private const string QueuePrefix = "unhandled/";

    public RabbitMqUnhandledRoute Resolve(string sourceQueueName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceQueueName);

        var unhandledQueueName = $"{QueuePrefix}{sourceQueueName}";

        return new RabbitMqUnhandledRoute(ExchangeName, unhandledQueueName, unhandledQueueName);
    }
}