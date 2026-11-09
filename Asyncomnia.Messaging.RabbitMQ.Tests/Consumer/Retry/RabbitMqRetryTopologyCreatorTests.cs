using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Routing;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.Retry;

public sealed class RabbitMqRetryTopologyCreatorTests
{
    [Fact]
    public async Task DeclareAsync_WhenBrokerRetriesAreDisabled_DoesNotCreateChannel()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var creator = CreateCreator(
            channelFactory,
            new RabbitMqConsumerOptions
            {
                EndpointName = "billing-service",
                BrokerRetriesEnabled = false
            });

        await creator.DeclareAsync();

        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task DeclareAsync_DeclaresDistinctRetryTopologiesAndBindsConsumerQueuesToReturnExchange()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var options = new RabbitMqConsumerOptions
        {
            EndpointName = "billing-service",
            BrokerRetriesEnabled = true,
            BrokerRetryDelays =
            [
                TimeSpan.FromSeconds(30),
                TimeSpan.FromSeconds(30),
                TimeSpan.FromMinutes(1)
            ]
        };
        var creator = CreateCreator(
            channelFactory,
            options,
            eventTypes: [typeof(ClientCreatedEvent)],
            commandTypes: [typeof(StartLongWorkJobCommand)]);
        const string returnExchange = "asyncomnia/retry-return";
        var retryTopologies = new[]
        {
            new RetryTopology(
                "asyncomnia/retry-30000ms",
                "asyncomnia/retry-30000ms-queue",
                30_000L),
            new RetryTopology(
                "asyncomnia/retry-60000ms",
                "asyncomnia/retry-60000ms-queue",
                60_000L)
        };
        var consumerQueueNames = new[]
        {
            "events/clients/subscription/billing-service",
            "commands/long-work-job/subscription/billing-service"
        };
        SetupChannel(channelFactory, channel);
        SetupExchangeDeclaration(
            channel,
            returnExchange,
            ExchangeType.Direct);

        foreach (var retryTopology in retryTopologies)
        {
            SetupRetryTopologyDeclaration(
                channel,
                retryTopology,
                returnExchange);
        }

        foreach (var consumerQueueName in consumerQueueNames)
        {
            SetupQueueBinding(
                channel,
                consumerQueueName,
                returnExchange,
                consumerQueueName);
        }

        await creator.DeclareAsync();

        channel.Verify(
            x => x.ExchangeDeclareAsync(
                returnExchange,
                ExchangeType.Direct,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None),
            Times.Once);

        foreach (var retryTopology in retryTopologies)
        {
            channel.Verify(
                x => x.ExchangeDeclareAsync(
                    retryTopology.ExchangeName,
                    ExchangeType.Fanout,
                    true,
                    false,
                    null,
                    false,
                    false,
                    CancellationToken.None),
                Times.Once);
            channel.Verify(
                x => x.QueueDeclareAsync(
                    retryTopology.QueueName,
                    true,
                    false,
                    false,
                    It.Is<IDictionary<string, object?>>(arguments =>
                        HasRetryQueueArguments(
                            arguments,
                            retryTopology.DelayMilliseconds,
                            returnExchange)),
                    false,
                    false,
                    CancellationToken.None),
                Times.Once);
            channel.Verify(
                x => x.QueueBindAsync(
                    retryTopology.QueueName,
                    retryTopology.ExchangeName,
                    string.Empty,
                    null,
                    false,
                    CancellationToken.None),
                Times.Once);
        }

        foreach (var consumerQueueName in consumerQueueNames)
        {
            channel.Verify(
                x => x.QueueBindAsync(
                    consumerQueueName,
                    returnExchange,
                    consumerQueueName,
                    null,
                    false,
                    CancellationToken.None),
                Times.Once);
        }

        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task DeclareAsync_ForwardsCancellationTokenToEveryBrokerOperation()
    {
        using var cancellationTokenSource =
            new CancellationTokenSource();
        var token = cancellationTokenSource.Token;
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var options = new RabbitMqConsumerOptions
        {
            EndpointName = "billing-service",
            BrokerRetriesEnabled = true,
            BrokerRetryDelays = [TimeSpan.FromSeconds(30)]
        };
        var creator = CreateCreator(
            channelFactory,
            options,
            eventTypes: [typeof(ClientCreatedEvent)]);
        const string returnExchange = "asyncomnia/retry-return";
        var retryTopology = new RetryTopology(
            "asyncomnia/retry-30000ms",
            "asyncomnia/retry-30000ms-queue",
            30_000L);
        const string consumerQueueName =
            "events/clients/subscription/billing-service";
        SetupChannel(channelFactory, channel, token);
        SetupExchangeDeclaration(
            channel,
            returnExchange,
            ExchangeType.Direct,
            token);
        SetupRetryTopologyDeclaration(
            channel,
            retryTopology,
            returnExchange,
            token);
        SetupQueueBinding(
            channel,
            consumerQueueName,
            returnExchange,
            consumerQueueName,
            token);

        await creator.DeclareAsync(token);

        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task DeclareAsync_WhenDeclarationFails_DisposesChannelAndPropagatesException()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var options = new RabbitMqConsumerOptions
        {
            EndpointName = "billing-service",
            BrokerRetriesEnabled = true,
            BrokerRetryDelays = [TimeSpan.FromSeconds(30)]
        };
        var creator = CreateCreator(channelFactory, options);
        var declarationException =
            new InvalidOperationException("declaration failed");
        SetupChannel(channelFactory, channel);
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                "asyncomnia/retry-return",
                ExchangeType.Direct,
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

    private static RabbitMqRetryTopologyCreator CreateCreator(
        Mock<IRabbitMqChannelFactory> channelFactory,
        RabbitMqConsumerOptions options,
        IReadOnlyCollection<Type>? eventTypes = null,
        IReadOnlyCollection<Type>? commandTypes = null)
    {
        var catalog = new Mock<IMessageHandlerCatalog>(MockBehavior.Strict);
        catalog
            .SetupGet(x => x.EventTypes)
            .Returns(eventTypes ?? Array.Empty<Type>());
        catalog
            .SetupGet(x => x.CommandTypes)
            .Returns(commandTypes ?? Array.Empty<Type>());

        var subscriptions = new RabbitMqSubscriptionResolver(
            catalog.Object,
            new MessageRouteResolver(),
            Options.Create(options));

        return new RabbitMqRetryTopologyCreator(
            channelFactory.Object,
            Options.Create(options),
            subscriptions);
    }

    private static void SetupChannel(
        Mock<IRabbitMqChannelFactory> channelFactory,
        Mock<IChannel> channel,
        CancellationToken token = default)
    {
        channelFactory
            .Setup(x => x.CreateAsync(null, token))
            .ReturnsAsync(channel.Object);
        channel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
    }

    private static void SetupExchangeDeclaration(
        Mock<IChannel> channel,
        string exchangeName,
        string exchangeType,
        CancellationToken token = default)
    {
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                exchangeName,
                exchangeType,
                true,
                false,
                null,
                false,
                false,
                token))
            .Returns(Task.CompletedTask);
    }

    private static void SetupRetryTopologyDeclaration(
        Mock<IChannel> channel,
        RetryTopology topology,
        string returnExchange,
        CancellationToken token = default)
    {
        SetupExchangeDeclaration(
            channel,
            topology.ExchangeName,
            ExchangeType.Fanout,
            token);
        channel
            .Setup(x => x.QueueDeclareAsync(
                topology.QueueName,
                true,
                false,
                false,
                It.Is<IDictionary<string, object?>>(arguments =>
                    HasRetryQueueArguments(
                        arguments,
                        topology.DelayMilliseconds,
                        returnExchange)),
                false,
                false,
                token))
            .ReturnsAsync(
                new QueueDeclareOk(topology.QueueName, 0, 0));
        SetupQueueBinding(
            channel,
            topology.QueueName,
            topology.ExchangeName,
            string.Empty,
            token);
    }

    private static void SetupQueueBinding(
        Mock<IChannel> channel,
        string queueName,
        string exchangeName,
        string routingKey,
        CancellationToken token = default)
    {
        channel
            .Setup(x => x.QueueBindAsync(
                queueName,
                exchangeName,
                routingKey,
                null,
                false,
                token))
            .Returns(Task.CompletedTask);
    }

    private static bool HasRetryQueueArguments(
        IDictionary<string, object?> arguments,
        long delayMilliseconds,
        string returnExchange)
    {
        return arguments.Count == 2
            && arguments.TryGetValue(
                "x-message-ttl",
                out var messageTimeToLive)
            && Convert.ToInt64(messageTimeToLive) == delayMilliseconds
            && arguments.TryGetValue(
                "x-dead-letter-exchange",
                out var deadLetterExchange)
            && Equals(deadLetterExchange, returnExchange)
            && !arguments.ContainsKey("x-dead-letter-routing-key");
    }

    private sealed record RetryTopology(
        string ExchangeName,
        string QueueName,
        long DelayMilliseconds);
}
