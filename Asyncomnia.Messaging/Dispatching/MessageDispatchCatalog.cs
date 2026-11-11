using System.Collections.Frozen;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Internal;

namespace Asyncomnia.Messaging.Dispatching;

internal interface IMessageDispatchCatalog
{
    bool TryGet(string contractType, out MessageDispatchDescriptor? descriptor);
}

internal sealed class MessageDispatchCatalog : IMessageDispatchCatalog
{
    private readonly FrozenDictionary<string, MessageDispatchDescriptor> _descriptors;

    public MessageDispatchCatalog(IEnumerable<MessageDispatchDescriptor> descriptors)
    {
        ArgumentNullException.ThrowIfNull(descriptors);

        _descriptors = descriptors.ToFrozenDictionary(
            descriptor => ClrMessageTypeResolver.GetContractType(descriptor.MessageType),
            StringComparer.Ordinal);
    }

    public bool TryGet(string contractType, out MessageDispatchDescriptor? descriptor)
    {
        ArgumentNullException.ThrowIfNull(contractType);

        return _descriptors.TryGetValue(contractType, out descriptor);
    }
}