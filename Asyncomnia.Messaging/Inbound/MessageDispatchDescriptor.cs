namespace Asyncomnia.Messaging.Inbound;

internal sealed record MessageDispatchDescriptor(
    Type MessageType,
    Func<IInboundMessagePipeline, InboundMessageContext, CancellationToken, Task> DispatchAsync);