namespace Asyncomnia.Messaging.Inbound;

public sealed record InboundMessageContext(string ConsumerId, string ContractType, IMessage Message, MessageMetadata Metadata);