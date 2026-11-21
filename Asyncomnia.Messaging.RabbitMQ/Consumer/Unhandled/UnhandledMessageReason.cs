namespace Asyncomnia.Messaging.RabbitMQ.Consumer.Unhandled;

internal static class UnhandledMessageReason
{
    public const string MissingContractType = "missing-contract-type";

    public const string UnknownContract = "unknown-contract";

    public const string NotConfiguredForSubscription = "not-configured-for-subscription";
}