using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;
using EasyNetQ;
using EasyNetQ.Topology;
using Microsoft.Extensions.Options;
using Moq;
using RabbitQueue = EasyNetQ.Topology.Queue;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.DeadLettering;

public sealed class RabbitMqDeadLetterTopologyCreatorTests
{
    [Fact]
    public async Task DeclareAsync_WhenDeadLetteringIsDisabled_DoesNotCallEasyNetQ()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy =
            new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var creator = CreateCreator(
            advancedBus,
            exchangeDeclareStrategy,
            deadLetteringEnabled: false);

        await creator.DeclareAsync(
        [
            CreateSubscription(
                "events/clients/subscription/billing-service")
        ]);

        advancedBus.VerifyNoOtherCalls();
        exchangeDeclareStrategy.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task DeclareAsync_WithoutSubscriptions_DoesNotCallEasyNetQ()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy =
            new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var creator = CreateCreator(
            advancedBus,
            exchangeDeclareStrategy);

        await creator.DeclareAsync([]);

        advancedBus.VerifyNoOtherCalls();
        exchangeDeclareStrategy.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task DeclareAsync_WithSubscriptions_DeclaresDirectExchangeAndDurableQueueForEachSubscription()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy =
            new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var creator = CreateCreator(
            advancedBus,
            exchangeDeclareStrategy);
        var sourceQueueNames = new[]
        {
            "commands/long-work-job/subscription/worker-service",
            "events/clients/subscription/worker-service"
        };
        var deadLetterQueueNames = sourceQueueNames
            .Select(queueName => $"deadletter/{queueName}")
            .ToArray();
        var exchange = CreateExchange("asyncomnia/deadletter");
        var queueConfigurations =
            new Dictionary<string, Action<IQueueDeclareConfiguration>>(
                StringComparer.Ordinal);
        exchangeDeclareStrategy
            .Setup(x => x.DeclareExchangeAsync(
                exchange.Name,
                ExchangeType.Direct,
                CancellationToken.None))
            .ReturnsAsync(exchange);

        foreach (var queueName in deadLetterQueueNames)
        {
            advancedBus
                .Setup(x => x.QueueDeclareAsync(
                    queueName,
                    It.IsAny<Action<IQueueDeclareConfiguration>>(),
                    CancellationToken.None))
                .Callback<
                    string,
                    Action<IQueueDeclareConfiguration>,
                    CancellationToken>((name, configure, _) =>
                        queueConfigurations[name] = configure)
                .ReturnsAsync(CreateQueue(queueName));
            advancedBus
                .Setup(x => x.QueueBindAsync(
                    queueName,
                    exchange.Name,
                    queueName,
                    null,
                    CancellationToken.None))
                .Returns(Task.CompletedTask);
        }

        await creator.DeclareAsync(
            sourceQueueNames.Select(CreateSubscription).ToArray());

        Assert.Equal(2, queueConfigurations.Count);
        Assert.All(
            queueConfigurations.Values,
            AssertDurableQueueConfiguration);
        exchangeDeclareStrategy.Verify(
            x => x.DeclareExchangeAsync(
                exchange.Name,
                ExchangeType.Direct,
                CancellationToken.None),
            Times.Once);
        exchangeDeclareStrategy.VerifyAll();
        advancedBus.VerifyAll();
    }

    [Fact]
    public async Task DeclareAsync_WithDuplicateSourceQueues_DeclaresDeadLetterQueueOnce()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var exchangeDeclareStrategy =
            new Mock<IExchangeDeclareStrategy>(MockBehavior.Strict);
        var creator = CreateCreator(
            advancedBus,
            exchangeDeclareStrategy);
        var sourceQueueName =
            "events/clients/subscription/billing-service";
        var deadLetterQueueName = $"deadletter/{sourceQueueName}";
        var exchange = CreateExchange("asyncomnia/deadletter");
        exchangeDeclareStrategy
            .Setup(x => x.DeclareExchangeAsync(
                exchange.Name,
                ExchangeType.Direct,
                CancellationToken.None))
            .ReturnsAsync(exchange);
        advancedBus
            .Setup(x => x.QueueDeclareAsync(
                deadLetterQueueName,
                It.IsAny<Action<IQueueDeclareConfiguration>>(),
                CancellationToken.None))
            .ReturnsAsync(CreateQueue(deadLetterQueueName));
        advancedBus
            .Setup(x => x.QueueBindAsync(
                deadLetterQueueName,
                exchange.Name,
                deadLetterQueueName,
                null,
                CancellationToken.None))
            .Returns(Task.CompletedTask);

        await creator.DeclareAsync(
        [
            CreateSubscription(sourceQueueName),
            CreateSubscription(sourceQueueName)
        ]);

        exchangeDeclareStrategy.Verify(
            x => x.DeclareExchangeAsync(
                exchange.Name,
                ExchangeType.Direct,
                CancellationToken.None),
            Times.Once);
        advancedBus.Verify(
            x => x.QueueDeclareAsync(
                deadLetterQueueName,
                It.IsAny<Action<IQueueDeclareConfiguration>>(),
                CancellationToken.None),
            Times.Once);
        advancedBus.Verify(
            x => x.QueueBindAsync(
                deadLetterQueueName,
                exchange.Name,
                deadLetterQueueName,
                null,
                CancellationToken.None),
            Times.Once);
    }

    private static RabbitMqDeadLetterTopologyCreator CreateCreator(
        Mock<IAdvancedBus> advancedBus,
        Mock<IExchangeDeclareStrategy> exchangeDeclareStrategy,
        bool deadLetteringEnabled = true)
    {
        return new RabbitMqDeadLetterTopologyCreator(
            advancedBus.Object,
            exchangeDeclareStrategy.Object,
            new RabbitMqDeadLetterRouteResolver(),
            Options.Create(
                new RabbitMqConsumerOptions
                {
                    EndpointName = "test-service",
                    DeadLetteringEnabled = deadLetteringEnabled
                }));
    }

    private static RabbitMqDeclaredSubscription CreateSubscription(
        string queueName)
    {
        var subscription = new RabbitMqSubscription
        {
            ExchangeName = queueName.Split("/subscription/")[0],
            QueueName = queueName,
            ConsumerSettings = new RabbitMqConsumerSettings(1, 1, 1)
        };

        return new RabbitMqDeclaredSubscription(
            subscription,
            CreateQueue(queueName));
    }

    private static void AssertDurableQueueConfiguration(Action<IQueueDeclareConfiguration> configure)
    {
        var configuration =
            new Mock<IQueueDeclareConfiguration>(MockBehavior.Strict);
        configuration
            .Setup(x => x.AsDurable(true))
            .Returns(configuration.Object);
        configuration
            .Setup(x => x.AsAutoDelete(false))
            .Returns(configuration.Object);
        configuration
            .Setup(x => x.AsExclusive(false))
            .Returns(configuration.Object);

        configure(configuration.Object);

        configuration.VerifyAll();
    }

    private static Exchange CreateExchange(string name)
    {
        return new Exchange(
            name,
            ExchangeType.Direct,
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
