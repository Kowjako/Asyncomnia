namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal enum AckResponse
{
    Ack,
    NackWithRequeue,
    NackWithoutRequeue
}
