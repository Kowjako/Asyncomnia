namespace Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;

internal sealed record RabbitMqDeadLetterRoute(string ExchangeName, string QueueName, string RoutingKey);