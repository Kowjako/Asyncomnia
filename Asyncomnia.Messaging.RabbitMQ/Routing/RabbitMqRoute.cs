namespace Asyncomnia.Messaging.RabbitMQ.Routing;

internal sealed record RabbitMqRoute(string ExchangeName, string RoutingKey);