using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using Asyncomnia.Messaging.Inbound;
using EasyNetQ;
using EasyNetQ.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer;

public sealed class RabbitMqHandlerRegistrarTests
{
    [Fact]
    public async Task Register_TypedHandlers_InvokeMatchingInboundPipelineMethods()
    {
        var pipeline = new Mock<IInboundMessagePipeline>(MockBehavior.Strict);
        pipeline
            .Setup(x => x.HandleEventAsync(
                It.IsAny<MessageEnvelope<ClientCreatedEvent>>(),
                CancellationToken.None))
            .Returns(Task.CompletedTask);
        pipeline
            .Setup(x => x.HandleCommandAsync(
                It.IsAny<MessageEnvelope<StartLongWorkJobCommand>>(),
                CancellationToken.None))
            .Returns(Task.CompletedTask);
        var services = new ServiceCollection();
        services.AddSingleton(pipeline.Object);
        await using var provider = services.BuildServiceProvider();
        var messageConsumer = new RabbitMqMessageConsumer(
            provider.GetRequiredService<IServiceScopeFactory>());
        var registrar = new RabbitMqHandlerRegistrar(messageConsumer);
        IMessageHandler<ClientCreatedEvent>? eventHandler = null;
        IMessageHandler<StartLongWorkJobCommand>? commandHandler = null;
        var registration = new Mock<IHandlerRegistration>(MockBehavior.Strict);
        registration
            .SetupSet(x => x.ThrowOnNoMatchingHandler = true);
        registration
            .Setup(x => x.Add<ClientCreatedEvent>(
                It.IsAny<IMessageHandler<ClientCreatedEvent>>()))
            .Callback<IMessageHandler<ClientCreatedEvent>>(
                handler => eventHandler = handler)
            .Returns(registration.Object);
        registration
            .Setup(x => x.Add<StartLongWorkJobCommand>(
                It.IsAny<IMessageHandler<StartLongWorkJobCommand>>()))
            .Callback<IMessageHandler<StartLongWorkJobCommand>>(
                handler => commandHandler = handler)
            .Returns(registration.Object);
        registrar.Register(
            registration.Object,
            [
                new RabbitMqBinding("client-created-event", typeof(ClientCreatedEvent)),
                new RabbitMqBinding("start-long-work-job-command", typeof(StartLongWorkJobCommand))
            ]);
        var properties = new MessageProperties
        {
            MessageId = Guid.NewGuid().ToString("D"),
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
        };
        var receivedInfo = new MessageReceivedInfo(
            "consumer",
            1,
            false,
            "exchange",
            "routing-key",
            "queue");

        var eventAck = await eventHandler!(
            new Message<ClientCreatedEvent>(
                new ClientCreatedEvent("Acme"),
                properties),
            receivedInfo,
            CancellationToken.None);
        var commandAck = await commandHandler!(
            new Message<StartLongWorkJobCommand>(
                new StartLongWorkJobCommand(Guid.NewGuid()),
                properties),
            receivedInfo,
            CancellationToken.None);

        Assert.Same(AckStrategies.AckAsync, eventAck);
        Assert.Same(AckStrategies.AckAsync, commandAck);
        pipeline.VerifyAll();
        registration.VerifyAll();
    }

    [Fact]
    public void Register_WithEventAndCommand_RegistersTypedHandlersAndEnablesStrictMatching()
    {
        var registration = new Mock<IHandlerRegistration>(MockBehavior.Strict);
        registration
            .SetupSet(x => x.ThrowOnNoMatchingHandler = true);
        registration
            .Setup(x => x.Add<ClientCreatedEvent>(
                It.IsAny<IMessageHandler<ClientCreatedEvent>>()))
            .Returns(registration.Object);
        registration
            .Setup(x => x.Add<StartLongWorkJobCommand>(
                It.IsAny<IMessageHandler<StartLongWorkJobCommand>>()))
            .Returns(registration.Object);
        var registrar = CreateRegistrar();

        registrar.Register(
            registration.Object,
            [
                new RabbitMqBinding("client-created-event", typeof(ClientCreatedEvent)),
                new RabbitMqBinding("start-long-work-job-command", typeof(StartLongWorkJobCommand))
            ]);

        registration.VerifyAll();
    }

    [Fact]
    public void Register_WithDuplicateMessageType_RegistersHandlerOnce()
    {
        var registration = new Mock<IHandlerRegistration>(MockBehavior.Strict);
        registration
            .SetupSet(x => x.ThrowOnNoMatchingHandler = true);
        registration
            .Setup(x => x.Add<ClientCreatedEvent>(
                It.IsAny<IMessageHandler<ClientCreatedEvent>>()))
            .Returns(registration.Object);
        var registrar = CreateRegistrar();

        registrar.Register(
            registration.Object,
            [
                new RabbitMqBinding("first", typeof(ClientCreatedEvent)),
                new RabbitMqBinding("second", typeof(ClientCreatedEvent))
            ]);

        registration.Verify(
            x => x.Add<ClientCreatedEvent>(
                It.IsAny<IMessageHandler<ClientCreatedEvent>>()),
            Times.Once);
        registration.VerifyAll();
    }

    [Theory]
    [InlineData(typeof(AmbiguousMessage))]
    [InlineData(typeof(MessageWithoutKind))]
    public void Register_WithInvalidMessageKind_Throws(Type messageType)
    {
        var registration = new Mock<IHandlerRegistration>(MockBehavior.Strict);
        registration
            .SetupSet(x => x.ThrowOnNoMatchingHandler = true);
        var registrar = CreateRegistrar();

        var exception = Assert.Throws<InvalidOperationException>(
            () => registrar.Register(
                registration.Object,
                [new RabbitMqBinding("invalid", messageType)]));

        Assert.Contains(messageType.FullName!, exception.Message);
        registration.VerifyAll();
    }

    [Fact]
    public void Register_WhenRegistrationIsNull_Throws()
    {
        var registrar = CreateRegistrar();

        Assert.Throws<ArgumentNullException>(
            () => registrar.Register(null!, []));
    }

    [Fact]
    public void Register_WhenBindingsAreNull_Throws()
    {
        var registration = new Mock<IHandlerRegistration>(MockBehavior.Strict);
        var registrar = CreateRegistrar();

        Assert.Throws<ArgumentNullException>(
            () => registrar.Register(registration.Object, null!));

        registration.VerifyNoOtherCalls();
    }

    private static RabbitMqHandlerRegistrar CreateRegistrar()
    {
        var scopeFactory = new Mock<IServiceScopeFactory>(MockBehavior.Strict);
        var messageConsumer = new RabbitMqMessageConsumer(scopeFactory.Object);

        return new RabbitMqHandlerRegistrar(messageConsumer);
    }
}
