using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Sagas.Configuration;
using Asyncomnia.Messaging.Sagas.DependencyInjection;
using Asyncomnia.Messaging.Sagas.Persistence;
using Asyncomnia.Messaging.Sagas.Runtime;
using Asyncomnia.Messaging.Sagas.StateMachine;
using Asyncomnia.Messaging.Sagas.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Sagas.Tests.DependencyInjection;

public sealed class SagaRegistrationExtensionsTests
{
    [Fact]
    public void RegisterAsyncomniaSaga_WhenBuilderIsNull_ThrowsArgumentNullException()
    {
        AsyncomniaMessagingBuilder builder = null!;

        var exception = Assert.Throws<ArgumentNullException>(() =>
            builder.RegisterAsyncomniaSaga<RegistrationStateMachine, TestSagaInstance>());

        Assert.Equal("builder", exception.ParamName);
    }

    [Fact]
    public void RegisterAsyncomniaSaga_ReturnsSameBuilderAndRegistersStateMachineAndRuntime()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        var returnedBuilder = builder.RegisterAsyncomniaSaga<RegistrationStateMachine, TestSagaInstance>();

        Assert.Same(builder, returnedBuilder);

        var stateMachineRegistration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(RegistrationStateMachine));
        Assert.Equal(ServiceLifetime.Singleton, stateMachineRegistration.Lifetime);
        Assert.IsType<RegistrationStateMachine>(stateMachineRegistration.ImplementationInstance);

        var runtimeRegistration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(SagaRuntime<RegistrationStateMachine, TestSagaInstance>));
        Assert.Equal(ServiceLifetime.Scoped, runtimeRegistration.Lifetime);
    }

    [Fact]
    public void RegisterAsyncomniaSaga_RegistersEventAndCommandAdaptersAsScopedServices()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.RegisterAsyncomniaSaga<RegistrationStateMachine, TestSagaInstance>();

        var eventHandlerRegistration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IEventHandler<TestEvent>));
        Assert.Equal(ServiceLifetime.Scoped, eventHandlerRegistration.Lifetime);
        Assert.Equal(
            typeof(SagaEventHandler<RegistrationStateMachine, TestSagaInstance, TestEvent>),
            eventHandlerRegistration.ImplementationType);

        var commandHandlerRegistration = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(ICommandHandler<TestCommand>));
        Assert.Equal(ServiceLifetime.Scoped, commandHandlerRegistration.Lifetime);
        Assert.Equal(
            typeof(SagaCommandHandler<RegistrationStateMachine, TestSagaInstance, TestCommand>),
            commandHandlerRegistration.ImplementationType);
    }

    [Fact]
    public void RegisterAsyncomniaSaga_RegisteredAdaptersCanBeResolvedFromScope()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ISagaRepository<TestSagaInstance>>(
            new StubSagaRepository<TestSagaInstance>());
        services
            .AddAsyncomniaMessaging()
            .RegisterAsyncomniaSaga<RegistrationStateMachine, TestSagaInstance>();
        using var provider = services.BuildServiceProvider();
        using var firstScope = provider.CreateScope();
        using var secondScope = provider.CreateScope();

        var eventHandler = firstScope.ServiceProvider.GetRequiredService<IEventHandler<TestEvent>>();
        var commandHandler = firstScope.ServiceProvider.GetRequiredService<ICommandHandler<TestCommand>>();
        var firstRuntime = firstScope.ServiceProvider
            .GetRequiredService<SagaRuntime<RegistrationStateMachine, TestSagaInstance>>();
        var secondRuntime = secondScope.ServiceProvider
            .GetRequiredService<SagaRuntime<RegistrationStateMachine, TestSagaInstance>>();
        var firstStateMachine = firstScope.ServiceProvider.GetRequiredService<RegistrationStateMachine>();
        var secondStateMachine = secondScope.ServiceProvider.GetRequiredService<RegistrationStateMachine>();

        Assert.IsType<SagaEventHandler<RegistrationStateMachine, TestSagaInstance, TestEvent>>(eventHandler);
        Assert.IsType<SagaCommandHandler<RegistrationStateMachine, TestSagaInstance, TestCommand>>(commandHandler);
        Assert.NotSame(firstRuntime, secondRuntime);
        Assert.Same(firstStateMachine, secondStateMachine);
    }

    [Fact]
    public void RegisterAsyncomniaSaga_AddsMessagesToHandlerCatalog()
    {
        var services = new ServiceCollection();
        services
            .AddAsyncomniaMessaging()
            .RegisterAsyncomniaSaga<RegistrationStateMachine, TestSagaInstance>();

        using var provider = services.BuildServiceProvider();
        var catalog = provider.GetRequiredService<IMessageHandlerCatalog>();

        Assert.Contains(typeof(TestEvent), catalog.EventTypes);
        Assert.Contains(typeof(TestCommand), catalog.CommandTypes);
    }

    [Fact]
    public void RegisterAsyncomniaSaga_RegistersOneDispatchDescriptorPerMessageType()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.RegisterAsyncomniaSaga<RegistrationStateMachine, TestSagaInstance>();

        var descriptors = services
            .Where(descriptor => descriptor.ServiceType == typeof(MessageDispatchDescriptor))
            .Select(descriptor => Assert.IsType<MessageDispatchDescriptor>(descriptor.ImplementationInstance))
            .ToArray();

        Assert.Equal(2, descriptors.Length);
        Assert.Contains(descriptors, descriptor => descriptor.MessageType == typeof(TestEvent));
        Assert.Contains(descriptors, descriptor => descriptor.MessageType == typeof(TestCommand));
    }

    [Fact]
    public void RegisterAsyncomniaSaga_CalledTwice_DoesNotDuplicateHandlersOrMetadata()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.RegisterAsyncomniaSaga<RegistrationStateMachine, TestSagaInstance>();
        builder.RegisterAsyncomniaSaga<RegistrationStateMachine, TestSagaInstance>();

        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IEventHandler<TestEvent>));
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(ICommandHandler<TestCommand>));
        Assert.Equal(
            2,
            services.Count(descriptor => descriptor.ServiceType == typeof(MessageHandlerRegistration)));
        Assert.Equal(
            2,
            services.Count(descriptor => descriptor.ServiceType == typeof(MessageDispatchDescriptor)));
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IValidateOptions<SagaOptions>));
    }

    [Fact]
    public void RegisterAsyncomniaSaga_WhenTwoSagasConsumeSameEvent_RegistersBothHandlersAndSharedMetadataOnce()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.RegisterAsyncomniaSaga<RegistrationStateMachine, TestSagaInstance>();
        builder.RegisterAsyncomniaSaga<SecondEventStateMachine, SecondSagaInstance>();

        Assert.Equal(
            2,
            services.Count(descriptor => descriptor.ServiceType == typeof(IEventHandler<TestEvent>)));
        Assert.Single(
            services.Where(descriptor => descriptor.ServiceType == typeof(MessageHandlerRegistration))
                .Select(descriptor => Assert.IsType<MessageHandlerRegistration>(descriptor.ImplementationInstance))
                .Where(registration => registration.MessageType == typeof(TestEvent)));
        Assert.Single(
            services.Where(descriptor => descriptor.ServiceType == typeof(MessageDispatchDescriptor))
                .Select(descriptor => Assert.IsType<MessageDispatchDescriptor>(descriptor.ImplementationInstance))
                .Where(descriptor => descriptor.MessageType == typeof(TestEvent)));
    }

    [Fact]
    public void RegisterAsyncomniaSaga_WhenMessageImplementsEventAndCommand_ThrowsWithoutChangingServices()
    {
        var services = new ServiceCollection();
        var builder = new AsyncomniaMessagingBuilder(services);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            builder.RegisterAsyncomniaSaga<ConflictingMessageStateMachine, TestSagaInstance>());

        Assert.Contains(typeof(ConflictingMessageStateMachine).FullName!, exception.Message);
        Assert.Contains(typeof(ConflictingMessage).FullName!, exception.Message);
        Assert.Contains("both an event and a command", exception.Message);
        Assert.Empty(services);
    }

    [Fact]
    public void RegisterAsyncomniaSaga_WhenMessageIsNeitherEventNorCommand_ThrowsWithoutChangingServices()
    {
        var services = new ServiceCollection();
        var builder = new AsyncomniaMessagingBuilder(services);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            builder.RegisterAsyncomniaSaga<UnsupportedMessageStateMachine, TestSagaInstance>());

        Assert.Contains(typeof(UnsupportedMessageStateMachine).FullName!, exception.Message);
        Assert.Contains(typeof(UnsupportedMessage).FullName!, exception.Message);
        Assert.Contains("neither IEvent nor ICommand", exception.Message);
        Assert.Empty(services);
    }

    private sealed class RegistrationStateMachine : AsyncomniaStateMachine<TestSagaInstance>
    {
        public RegistrationStateMachine()
        {
            var testEvent = Event<TestEvent>(configuration =>
                configuration.CorrelateById(message => message.SagaId));
            var testCommand = Event<TestCommand>(configuration =>
                configuration.CorrelateById(message => message.SagaId));

            Initially(When(testEvent), When(testCommand));
        }
    }

    private sealed class SecondSagaInstance : AsyncomniaSagaInstance;

    private sealed class SecondEventStateMachine : AsyncomniaStateMachine<SecondSagaInstance>
    {
        public SecondEventStateMachine()
        {
            var testEvent = Event<TestEvent>(configuration =>
                configuration.CorrelateById(message => message.SagaId));

            Initially(When(testEvent));
        }
    }

    private sealed record ConflictingMessage(Guid SagaId) : IEvent, ICommand;

    private sealed class ConflictingMessageStateMachine : AsyncomniaStateMachine<TestSagaInstance>
    {
        public ConflictingMessageStateMachine()
        {
            Event<ConflictingMessage>(configuration =>
                configuration.CorrelateById(message => message.SagaId));
        }
    }

    private sealed record UnsupportedMessage(Guid SagaId) : IMessage;

    private sealed class UnsupportedMessageStateMachine : AsyncomniaStateMachine<TestSagaInstance>
    {
        public UnsupportedMessageStateMachine()
        {
            Event<UnsupportedMessage>(configuration =>
                configuration.CorrelateById(message => message.SagaId));
        }
    }
}
