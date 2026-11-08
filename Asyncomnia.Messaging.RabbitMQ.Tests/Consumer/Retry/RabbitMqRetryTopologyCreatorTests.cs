using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using Asyncomnia.Messaging.RabbitMQ.Routing;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using EasyNetQ;
using EasyNetQ.Topology;
using Microsoft.Extensions.Options;
using Moq;
using RabbitQueue = EasyNetQ.Topology.Queue;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.Retry;

public sealed class RabbitMqRetryTopologyCreatorTests
{
    [Fact]
    public async Task DeclareAsync_WhenBrokerRetriesAreDisabled_DoesNotCallEasyNetQ()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy =
            new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var creator = CreateCreator(
            advancedBus,
            exchangeDeclareStrategy,
            new RabbitMqConsumerOptions
            {
                EndpointName = "billing-service",
                BrokerRetriesEnabled = false
            });

        await creator.DeclareAsync();

        advancedBus.VerifyNoOtherCalls();
        exchangeDeclareStrategy.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task DeclareAsync_DeclaresDistinctRetryTopologiesAndBindsConsumerQueueToReturnExchange()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy =
            new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
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
            advancedBus,
            exchangeDeclareStrategy,
            options,
            eventTypes: [typeof(ClientCreatedEvent)]);
        var returnExchange = CreateExchange(
            "asyncomnia/retry-return",
            ExchangeType.Direct);
        var retry30SecondsExchange = CreateExchange(
            "asyncomnia/retry-30000ms",
            ExchangeType.Fanout);
        var retry1MinuteExchange = CreateExchange(
            "asyncomnia/retry-60000ms",
            ExchangeType.Fanout);
        var retry30SecondsQueue = CreateQueue(
            "asyncomnia/retry-30000ms-queue");
        var retry1MinuteQueue = CreateQueue(
            "asyncomnia/retry-60000ms-queue");
        var queueConfigurations =
            new Dictionary<string, Action<IQueueDeclareConfiguration>>();

        SetupExchangeDeclaration(
            exchangeDeclareStrategy,
            returnExchange,
            ExchangeType.Direct);
        SetupExchangeDeclaration(
            exchangeDeclareStrategy,
            retry30SecondsExchange,
            ExchangeType.Fanout);
        SetupExchangeDeclaration(
            exchangeDeclareStrategy,
            retry1MinuteExchange,
            ExchangeType.Fanout);
        SetupQueueDeclaration(
            advancedBus,
            retry30SecondsQueue,
            queueConfigurations);
        SetupQueueDeclaration(
            advancedBus,
            retry1MinuteQueue,
            queueConfigurations);
        SetupQueueBinding(
            advancedBus,
            retry30SecondsQueue.Name,
            retry30SecondsExchange.Name,
            string.Empty);
        SetupQueueBinding(
            advancedBus,
            retry1MinuteQueue.Name,
            retry1MinuteExchange.Name,
            string.Empty);
        SetupQueueBinding(
            advancedBus,
            "events/clients/subscription/billing-service",
            returnExchange.Name,
            "events/clients/subscription/billing-service");

        await creator.DeclareAsync();

        Assert.Equal(2, queueConfigurations.Count);
        AssertQueueConfiguration(
            queueConfigurations[retry30SecondsQueue.Name],
            TimeSpan.FromSeconds(30),
            returnExchange.Name);
        AssertQueueConfiguration(
            queueConfigurations[retry1MinuteQueue.Name],
            TimeSpan.FromMinutes(1),
            returnExchange.Name);
        exchangeDeclareStrategy.Verify(
            x => x.DeclareExchangeAsync(
                retry30SecondsExchange.Name,
                ExchangeType.Fanout,
                CancellationToken.None),
            Times.Once);
        advancedBus.Verify(
            x => x.QueueDeclareAsync(
                retry30SecondsQueue.Name,
                It.IsAny<Action<IQueueDeclareConfiguration>>(),
                CancellationToken.None),
            Times.Once);
        exchangeDeclareStrategy.VerifyAll();
        advancedBus.VerifyAll();
    }

    private static RabbitMqRetryTopologyCreator CreateCreator(
        Mock<IAdvancedBus> advancedBus,
        Mock<IExchangeDeclareStrategy> exchangeDeclareStrategy,
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
            advancedBus.Object,
            exchangeDeclareStrategy.Object,
            Options.Create(options),
            subscriptions);
    }

    private static void SetupExchangeDeclaration(
        Mock<IExchangeDeclareStrategy> exchangeDeclareStrategy,
        Exchange exchange,
        string exchangeType)
    {
        exchangeDeclareStrategy
            .Setup(x => x.DeclareExchangeAsync(
                exchange.Name,
                exchangeType,
                CancellationToken.None))
            .ReturnsAsync(exchange);
    }

    private static void SetupQueueDeclaration(
        Mock<IAdvancedBus> advancedBus,
        RabbitQueue queue,
        IDictionary<string, Action<IQueueDeclareConfiguration>> queueConfigurations)
    {
        advancedBus
            .Setup(x => x.QueueDeclareAsync(
                queue.Name,
                It.IsAny<Action<IQueueDeclareConfiguration>>(),
                CancellationToken.None))
            .Callback<string, Action<IQueueDeclareConfiguration>, CancellationToken>(
                (_, configure, _) => queueConfigurations.Add(queue.Name, configure))
            .ReturnsAsync(queue);
    }

    private static void SetupQueueBinding(
        Mock<IAdvancedBus> advancedBus,
        string queueName,
        string exchangeName,
        string routingKey)
    {
        advancedBus
            .Setup(x => x.QueueBindAsync(
                queueName,
                exchangeName,
                routingKey,
                null,
                CancellationToken.None))
            .Returns(Task.CompletedTask);
    }

    private static void AssertQueueConfiguration(
        Action<IQueueDeclareConfiguration> configure,
        TimeSpan delay,
        string returnExchangeName)
    {
        var queueConfiguration =
            new Mock<IQueueDeclareConfiguration>(MockBehavior.Loose);

        configure(queueConfiguration.Object);

        queueConfiguration.Verify(
            x => x.AsDurable(true),
            Times.Once);
        queueConfiguration.Verify(
            x => x.AsAutoDelete(false),
            Times.Once);
        queueConfiguration.Verify(
            x => x.AsExclusive(false),
            Times.Once);
        queueConfiguration.Verify(
            x => x.WithArgument(
                "x-message-ttl",
                It.Is<object>(value =>
                    Convert.ToInt64(value) == (long)delay.TotalMilliseconds)),
            Times.Once);
        queueConfiguration.Verify(
            x => x.WithArgument(
                "x-dead-letter-exchange",
                It.Is<object>(value =>
                    Equals(value, returnExchangeName))),
            Times.Once);
        queueConfiguration.Verify(
            x => x.WithArgument(
                "x-dead-letter-routing-key",
                It.IsAny<object>()),
            Times.Never);
    }

    private static Exchange CreateExchange(string name, string exchangeType)
    {
        return new Exchange(
            name,
            exchangeType,
            true,
            false,
            new Dictionary<string, object>());
    }

    private static RabbitQueue CreateQueue(string name)
    {
        return new RabbitQueue(
            name,
            true,
            false,
            false,
            new Dictionary<string, object>());
    }
}