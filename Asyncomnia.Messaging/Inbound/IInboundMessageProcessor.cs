using Asyncomnia.Messaging.Dispatching;

namespace Asyncomnia.Messaging.Inbound;

public interface IInboundMessageProcessor
{
    Task ProcessAsync(InboundMessageContext context, CancellationToken token = default);
}

internal sealed class InboundMessageProcessor(IMessageDispatchCatalog dispatchCatalog, IInboundMessagePipeline pipeline) : IInboundMessageProcessor
{
    private readonly IMessageDispatchCatalog _dispatchCatalog = dispatchCatalog
        ?? throw new ArgumentNullException(nameof(dispatchCatalog));

    private readonly IInboundMessagePipeline _pipeline = pipeline
        ?? throw new ArgumentNullException(nameof(pipeline));

    public Task ProcessAsync(InboundMessageContext context, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!_dispatchCatalog.TryGet(context.ContractType, out var descriptor) || descriptor is null)
        {
            throw new InvalidOperationException($"No dispatch descriptor registered for contract '{context.ContractType}'");
        }

        return descriptor.DispatchAsync(_pipeline, context.Message, context.Metadata, token);
    }
}