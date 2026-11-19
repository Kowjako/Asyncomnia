namespace Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;

internal static class RabbitMqExchangeArgumentsFactory
{
    public static IDictionary<string, object?>? Create(bool useUnroutableFallback)
    {
        if (!useUnroutableFallback)
        {
            return null;
        }

        return new Dictionary<string, object?>
        {
            ["alternate-exchange"] = RabbitMqUnroutableDefaults.ExchangeName
        };
    }
}