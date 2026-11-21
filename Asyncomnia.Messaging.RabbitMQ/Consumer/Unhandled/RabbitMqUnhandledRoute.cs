namespace Asyncomnia.Messaging.RabbitMQ.Consumer.Unhandled;

internal sealed record RabbitMqUnhandledRoute(string ExchangeName, string QueueName, string RoutingKey);