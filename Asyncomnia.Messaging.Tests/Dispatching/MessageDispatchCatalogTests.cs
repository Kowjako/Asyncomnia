using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Internal;
using Asyncomnia.Messaging.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Tests.Dispatching;

public sealed class MessageDispatchCatalogTests
{
    [Fact]
    public void TryGet_WithRegisteredContract_ReturnsDescriptor()
    {
        var expected = MessageDispatchDescriptorFactory.CreateEvent<TestEvent>();
        var catalog = new MessageDispatchCatalog([expected]);

        var found = catalog.TryGet(
            ClrMessageTypeResolver.GetContractType(typeof(TestEvent)),
            out var descriptor);

        Assert.True(found);
        Assert.Same(expected, descriptor);
    }

    [Fact]
    public void TryGet_WithUnknownContract_ReturnsFalse()
    {
        var catalog = new MessageDispatchCatalog([]);

        var found = catalog.TryGet("Unknown.Message, Unknown.Assembly", out var descriptor);

        Assert.False(found);
        Assert.Null(descriptor);
    }

    [Fact]
    public void Constructor_WithDuplicateContract_Throws()
    {
        var first = MessageDispatchDescriptorFactory.CreateEvent<TestEvent>();
        var second = MessageDispatchDescriptorFactory.CreateEvent<TestEvent>();

        Assert.Throws<ArgumentException>(() => new MessageDispatchCatalog([first, second]));
    }
}
