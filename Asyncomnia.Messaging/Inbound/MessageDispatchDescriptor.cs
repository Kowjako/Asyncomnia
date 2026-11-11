namespace Asyncomnia.Messaging.Inbound;

internal sealed record MessageDispatchDescriptor(
    Type MessageType,
    Func<IInboundMessagePipeline, IMessage, MessageMetadata, CancellationToken, Task> DispatchAsync);