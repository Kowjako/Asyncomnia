using EasyNetQ.Topology;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;

internal sealed record RabbitMqDeclaredSubscription(RabbitMqSubscription Subscription, Queue Queue);