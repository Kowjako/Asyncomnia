using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Tests.Handlers;

public sealed class MessageHandlerCatalogTests
{
    [Fact]
    public void AddAsyncomniaMessaging_WithoutHandlers_RegistersEmptyCatalog()
    {
        var services = new ServiceCollection();
        services.AddAsyncomniaMessaging();
        using var provider = services.BuildServiceProvider();

        var catalog = provider.GetRequiredService<IMessageHandlerCatalog>();

        Assert.Empty(catalog.EventTypes);
        Assert.Empty(catalog.CommandTypes);
    }

    [Fact]
    public void RegisterHandlers_AddsEventAndCommandTypesToCatalog()
    {
        var services = new ServiceCollection();
        services
            .AddAsyncomniaMessaging()
            .RegisterEventHandler<TestEvent, FirstEventHandler>()
            .RegisterCommandHandler<TestCommand, FirstCommandHandler>();
        using var provider = services.BuildServiceProvider();

        var catalog = provider.GetRequiredService<IMessageHandlerCatalog>();

        Assert.Equal([typeof(TestEvent)], catalog.EventTypes);
        Assert.Equal([typeof(TestCommand)], catalog.CommandTypes);
    }

    [Fact]
    public void RegisterEventHandler_WithMultipleHandlers_AddsEventTypeOnce()
    {
        var services = new ServiceCollection();
        services
            .AddAsyncomniaMessaging()
            .RegisterEventHandler<TestEvent, FirstEventHandler>()
            .RegisterEventHandler<TestEvent, SecondEventHandler>();
        using var provider = services.BuildServiceProvider();

        var catalog = provider.GetRequiredService<IMessageHandlerCatalog>();

        Assert.Equal([typeof(TestEvent)], catalog.EventTypes);
    }

    [Fact]
    public void RegisterHandlersFromAssemblyContaining_AddsAllHandledMessageTypesToCatalog()
    {
        var services = new ServiceCollection();
        services
            .AddAsyncomniaMessaging()
            .RegisterHandlersFromAssemblyContaining<MessageHandlerCatalogTests>();
        using var provider = services.BuildServiceProvider();

        var catalog = provider.GetRequiredService<IMessageHandlerCatalog>();

        Type[] expectedEventTypes =
        [
            typeof(ScannedEvent),
            typeof(FirstMultiScannedEvent),
            typeof(SecondMultiScannedEvent)
        ];

        Assert.Equal(
            expectedEventTypes.OrderBy(x => x.FullName),
            catalog.EventTypes.OrderBy(x => x.FullName));
        Assert.Equal([typeof(ScannedCommand)], catalog.CommandTypes);
    }

    [Fact]
    public void RegisterHandlersFromAssemblyContaining_WhenCalledTwice_DoesNotDuplicateMessageTypes()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();
        builder.RegisterHandlersFromAssemblyContaining<MessageHandlerCatalogTests>();
        builder.RegisterHandlersFromAssemblyContaining<MessageHandlerCatalogTests>();
        using var provider = services.BuildServiceProvider();

        var catalog = provider.GetRequiredService<IMessageHandlerCatalog>();

        Assert.Equal(3, catalog.EventTypes.Count);
        Assert.Single(catalog.CommandTypes);
    }
}
