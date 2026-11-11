using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Internal;
using Asyncomnia.Messaging.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Tests.DependencyInjection;

public sealed class HandlerScanningExtensionsTests
{
    [Fact]
    public void RegisterHandlersFromAssemblyContaining_RegistersEventHandler()
    {
        var services = new ServiceCollection();
        services
            .AddAsyncomniaMessaging()
            .RegisterHandlersFromAssemblyContaining<HandlerScanningExtensionsTests>();

        using var provider = services.BuildServiceProvider();
        var handlers = provider.GetServices<IEventHandler<ScannedEvent>>().ToArray();

        Assert.IsType<ScannedEventHandler>(Assert.Single(handlers));
    }

    [Fact]
    public void RegisterHandlersFromAssemblyContaining_RegistersCommandHandler()
    {
        var services = new ServiceCollection();
        services
            .AddAsyncomniaMessaging()
            .RegisterHandlersFromAssemblyContaining<HandlerScanningExtensionsTests>();

        using var provider = services.BuildServiceProvider();
        var handlers = provider.GetServices<ICommandHandler<ScannedCommand>>().ToArray();

        Assert.IsType<ScannedCommandHandler>(Assert.Single(handlers));
    }

    [Fact]
    public void RegisterHandlersFromAssemblyContaining_IgnoresUnrelatedTypes()
    {
        var services = new ServiceCollection();
        services
            .AddAsyncomniaMessaging()
            .RegisterHandlersFromAssemblyContaining<HandlerScanningExtensionsTests>();

        using var provider = services.BuildServiceProvider();

        Assert.Null(provider.GetService<UnrelatedScannedType>());
    }

    [Fact]
    public void RegisterHandlersFromAssemblyContaining_WhenCalledTwice_DoesNotDuplicateHandlers()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.RegisterHandlersFromAssemblyContaining<HandlerScanningExtensionsTests>();
        builder.RegisterHandlersFromAssemblyContaining<HandlerScanningExtensionsTests>();

        using var provider = services.BuildServiceProvider();
        var handlers = provider.GetServices<IEventHandler<ScannedEvent>>().ToArray();

        Assert.IsType<ScannedEventHandler>(Assert.Single(handlers));
    }

    [Fact]
    public void RegisterHandlersFromAssemblyContaining_RegistersAllImplementedHandlerContracts()
    {
        var services = new ServiceCollection();
        services
            .AddAsyncomniaMessaging()
            .RegisterHandlersFromAssemblyContaining<HandlerScanningExtensionsTests>();

        using var provider = services.BuildServiceProvider();
        var firstHandlers = provider.GetServices<IEventHandler<FirstMultiScannedEvent>>().ToArray();
        var secondHandlers = provider.GetServices<IEventHandler<SecondMultiScannedEvent>>().ToArray();

        Assert.IsType<MultiScannedEventHandler>(Assert.Single(firstHandlers));
        Assert.IsType<MultiScannedEventHandler>(Assert.Single(secondHandlers));
    }

    [Fact]
    public void RegisterHandlersFromAssemblyContaining_RegistersDispatchDescriptorsForDiscoveredMessages()
    {
        var services = new ServiceCollection();
        services
            .AddAsyncomniaMessaging()
            .RegisterHandlersFromAssemblyContaining<HandlerScanningExtensionsTests>();
        using var provider = services.BuildServiceProvider();
        var catalog = provider.GetRequiredService<IMessageDispatchCatalog>();

        AssertDescriptor<ScannedEvent>(catalog);
        AssertDescriptor<ScannedCommand>(catalog);
        AssertDescriptor<FirstMultiScannedEvent>(catalog);
        AssertDescriptor<SecondMultiScannedEvent>(catalog);
    }

    [Fact]
    public void RegisterHandlersFromAssemblyContaining_WhenCalledTwice_RegistersOneDescriptorPerMessageType()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();
        builder.RegisterHandlersFromAssemblyContaining<HandlerScanningExtensionsTests>();
        builder.RegisterHandlersFromAssemblyContaining<HandlerScanningExtensionsTests>();

        var descriptors = services
            .Where(x => x.ServiceType == typeof(MessageDispatchDescriptor))
            .Select(x => Assert.IsType<MessageDispatchDescriptor>(x.ImplementationInstance))
            .ToArray();

        Assert.Equal(4, descriptors.Length);
        Assert.Equal(4, descriptors.Select(x => x.MessageType).Distinct().Count());
    }

    private static void AssertDescriptor<TMessage>(IMessageDispatchCatalog catalog)
        where TMessage : class, IMessage
    {
        Assert.True(
            catalog.TryGet(
                ClrMessageTypeResolver.GetContractType(typeof(TMessage)),
                out var descriptor));
        Assert.NotNull(descriptor);
        Assert.Equal(typeof(TMessage), descriptor!.MessageType);
    }
}
