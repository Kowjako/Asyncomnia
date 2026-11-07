namespace Asyncomnia.Messaging.RabbitMQ.Routing;

internal sealed record MessageRoute(string ExchangeName, string RoutingKey);