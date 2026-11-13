using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Routing;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.Topology;

public sealed class RabbitMqTopologyCreatorTests
{
    [Fact]
    public async Task DeclareAsync_WithoutSubscriptions_ReturnsEmptyCollectionWithoutCreatingChannel()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var creator = CreateCreator(channelFactory);

        var subscriptions = await creator.DeclareAsync();

        Assert.Empty(subscriptions);
        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task DeclareAsync_WithMultipleBindings_DeclaresDurableTopologyAndReturnsSubscription()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var creator = CreateCreator(
            channelFactory,
            eventTypes:
            [
                typeof(ClientCreatedEvent),
                typeof(ClientContractSignedEvent)
            ]);
        SetupChannel(channelFactory, channel);
        SetupExchangeDeclaration(channel, "events/clients");
        SetupQueueDeclaration(
            channel,
            "events/clients/subscription/test-service");
        SetupBinding(
            channel,
            "events/clients/subscription/test-service",
            "events/clients",
            "client-contract-signed");
        SetupBinding(
            channel,
            "events/clients/subscription/test-service",
            "events/clients",
            "client-created-event");

        var subscriptions = await creator.DeclareAsync();

        var subscription = Assert.Single(subscriptions);
        Assert.Equal("events/clients", subscription.ExchangeName);
        Assert.Equal(
            "events/clients/subscription/test-service",
            subscription.QueueName);
        Assert.Equal(
            ["client-contract-signed", "client-created-event"],
            subscription.Bindings.Select(x => x.RoutingKey));
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task DeclareAsync_WithEventAndCommand_DeclaresSeparateTopologiesUsingSingleChannel()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var creator = CreateCreator(
            channelFactory,
            eventTypes: [typeof(ClientCreatedEvent)],
            commandTypes: [typeof(StartLongWorkJobCommand)]);
        SetupChannel(channelFactory, channel);
        SetupTopology(
            channel,
            "commands/long-work-job",
            "commands/long-work-job/subscription/test-service",
            "start-long-work-job-command");
        SetupTopology(
            channel,
            "events/clients",
            "events/clients/subscription/test-service",
            "client-created-event");

        var subscriptions = await creator.DeclareAsync();

        Assert.Equal(2, subscriptions.Count);
        channelFactory.Verify(
            x => x.CreateAsync(null, CancellationToken.None),
            Times.Once);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task DeclareAsync_WithSingleActiveConsumer_DeclaresQueueWithSacArgument()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var creator = CreateCreator(
            channelFactory,
            eventTypes: [typeof(ClientCreatedEvent)],
            configureOptions: options => options.ConfigureSubscription(
                "events/clients",
                subscription => subscription.SingleActiveConsumer = true));
        SetupChannel(channelFactory, channel);
        SetupExchangeDeclaration(channel, "events/clients");
        SetupSingleActiveQueueDeclaration(
            channel,
            "events/clients/subscription/test-service");
        SetupBinding(
            channel,
            "events/clients/subscription/test-service",
            "events/clients",
            "client-created-event");

        var subscriptions = await creator.DeclareAsync();

        Assert.True(Assert.Single(subscriptions).Settings.SingleActiveConsumer);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task DeclareAsync_WhenCancellationIsAlreadyRequested_DoesNotCreateChannel()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var creator = CreateCreator(
            channelFactory,
            eventTypes: [typeof(ClientCreatedEvent)]);
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => creator.DeclareAsync(cancellationSource.Token));

        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task DeclareAsync_WhenDeclarationFails_DisposesChannelAndPropagatesException()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var declarationException =
            new InvalidOperationException("declaration failed");
        var creator = CreateCreator(
            channelFactory,
            eventTypes: [typeof(ClientCreatedEvent)]);
        SetupChannel(channelFactory, channel);
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                "events/clients",
                ExchangeType.Topic,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None))
            .ThrowsAsync(declarationException);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => creator.DeclareAsync());

        Assert.Same(declarationException, exception);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public void Constructor_WithNullDependencies_Throws()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var subscriptionResolver = CreateSubscriptionResolver();

        Assert.Throws<ArgumentNullException>(
            () => new RabbitMqTopologyCreator(
                null!,
                subscriptionResolver));
        Assert.Throws<ArgumentNullException>(
            () => new RabbitMqTopologyCreator(
                channelFactory.Object,
                null!));
    }

    private static RabbitMqTopologyCreator CreateCreator(
        Mock<IRabbitMqChannelFactory> channelFactory,
        IReadOnlyCollection<Type>? eventTypes = null,
        IReadOnlyCollection<Type>? commandTypes = null,
        Action<RabbitMqConsumerOptions>? configureOptions = null)
    {
        return new RabbitMqTopologyCreator(
            channelFactory.Object,
            CreateSubscriptionResolver(
                eventTypes,
                commandTypes,
                configureOptions));
    }

    private static RabbitMqSubscriptionResolver CreateSubscriptionResolver(
        IReadOnlyCollection<Type>? eventTypes = null,
        IReadOnlyCollection<Type>? commandTypes = null,
        Action<RabbitMqConsumerOptions>? configureOptions = null)
    {
        var catalog =
            new Mock<IMessageHandlerCatalog>(MockBehavior.Strict);
        catalog
            .SetupGet(x => x.EventTypes)
            .Returns(eventTypes ?? Array.Empty<Type>());
        catalog
            .SetupGet(x => x.CommandTypes)
            .Returns(commandTypes ?? Array.Empty<Type>());

        var options = new RabbitMqConsumerOptions
        {
            EndpointName = "test-service"
        };
        configureOptions?.Invoke(options);

        return new RabbitMqSubscriptionResolver(
            catalog.Object,
            new MessageRouteResolver(),
            Options.Create(options));
    }

    private static void SetupChannel(Mock<IRabbitMqChannelFactory> channelFactory, Mock<IChannel> channel)
    {
        channelFactory
            .Setup(x => x.CreateAsync(null, CancellationToken.None))
            .ReturnsAsync(channel.Object);
        channel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
    }

    private static void SetupTopology(Mock<IChannel> channel, string exchangeName, string queueName, string routingKey)
    {
        SetupExchangeDeclaration(channel, exchangeName);
        SetupQueueDeclaration(channel, queueName);
        SetupBinding(
            channel,
            queueName,
            exchangeName,
            routingKey);
    }

    private static void SetupExchangeDeclaration(Mock<IChannel> channel, string exchangeName)
    {
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                exchangeName,
                ExchangeType.Topic,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None))
            .Returns(Task.CompletedTask);
    }

    private static void SetupQueueDeclaration(Mock<IChannel> channel, string queueName)
    {
        channel
            .Setup(x => x.QueueDeclareAsync(
                queueName,
                true,
                false,
                false,
                null,
                false,
                false,
                CancellationToken.None))
            .ReturnsAsync(new QueueDeclareOk(queueName, 0, 0));
    }

    private static void SetupSingleActiveQueueDeclaration(
        Mock<IChannel> channel,
        string queueName)
    {
        channel
            .Setup(x => x.QueueDeclareAsync(
                queueName,
                true,
                false,
                false,
                It.Is<IDictionary<string, object?>?>(
                    arguments => HasSingleActiveConsumer(arguments)),
                false,
                false,
                CancellationToken.None))
            .ReturnsAsync(new QueueDeclareOk(queueName, 0, 0));
    }

    private static bool HasSingleActiveConsumer(
        IDictionary<string, object?>? arguments)
    {
        return arguments is not null &&
               arguments.Count == 1 &&
               arguments.TryGetValue("x-single-active-consumer", out var value) &&
               value is true;
    }

    private static void SetupBinding(Mock<IChannel> channel, string queueName, string exchangeName, string routingKey)
    {
        channel
            .Setup(x => x.QueueBindAsync(
                queueName,
                exchangeName,
                routingKey,
                null,
                false,
                CancellationToken.None))
            .Returns(Task.CompletedTask);
    }
}
