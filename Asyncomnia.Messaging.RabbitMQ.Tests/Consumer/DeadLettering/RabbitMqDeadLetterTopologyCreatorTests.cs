using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.DeadLettering;

public sealed class RabbitMqDeadLetterTopologyCreatorTests
{
    [Fact]
    public async Task DeclareAsync_WhenDeadLetteringIsDisabled_DoesNotCreateChannel()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var creator = CreateCreator(
            channelFactory,
            deadLetteringEnabled: false);

        await creator.DeclareAsync(
        [
            CreateSubscription(
                "events/clients/subscription/billing-service")
        ]);

        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task DeclareAsync_WithoutSubscriptions_DoesNotCreateChannel()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var creator = CreateCreator(channelFactory);

        await creator.DeclareAsync([]);

        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task DeclareAsync_WithSubscriptions_DeclaresDirectExchangeAndDurableQueueForEachSubscription()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var creator = CreateCreator(channelFactory);
        var sourceQueueNames = new[]
        {
            "commands/long-work-job/subscription/worker-service",
            "events/clients/subscription/worker-service"
        };
        var deadLetterQueueNames = sourceQueueNames
            .Select(queueName => $"deadletter/{queueName}")
            .ToArray();
        const string exchangeName = "asyncomnia/deadletter";
        SetupChannel(channelFactory, channel);
        SetupExchangeDeclaration(channel, exchangeName);

        foreach (var queueName in deadLetterQueueNames)
        {
            SetupQueueDeclaration(
                channel,
                queueName,
                exchangeName);
        }

        await creator.DeclareAsync(
            sourceQueueNames.Select(CreateSubscription).ToArray());

        channel.Verify(
            x => x.ExchangeDeclareAsync(
                exchangeName,
                ExchangeType.Direct,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None),
            Times.Once);

        foreach (var queueName in deadLetterQueueNames)
        {
            channel.Verify(
                x => x.QueueDeclareAsync(
                    queueName,
                    true,
                    false,
                    false,
                    null,
                    false,
                    false,
                    CancellationToken.None),
                Times.Once);
            channel.Verify(
                x => x.QueueBindAsync(
                    queueName,
                    exchangeName,
                    queueName,
                    null,
                    false,
                    CancellationToken.None),
                Times.Once);
        }

        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task DeclareAsync_WithDuplicateSourceQueues_DeclaresDeadLetterQueueOnce()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var creator = CreateCreator(channelFactory);
        var sourceQueueName =
            "events/clients/subscription/billing-service";
        var deadLetterQueueName = $"deadletter/{sourceQueueName}";
        const string exchangeName = "asyncomnia/deadletter";
        SetupChannel(channelFactory, channel);
        SetupExchangeDeclaration(channel, exchangeName);
        SetupQueueDeclaration(
            channel,
            deadLetterQueueName,
            exchangeName);

        await creator.DeclareAsync(
        [
            CreateSubscription(sourceQueueName),
            CreateSubscription(sourceQueueName)
        ]);

        channel.Verify(
            x => x.QueueDeclareAsync(
                deadLetterQueueName,
                true,
                false,
                false,
                null,
                false,
                false,
                CancellationToken.None),
            Times.Once);
        channel.Verify(
            x => x.QueueBindAsync(
                deadLetterQueueName,
                exchangeName,
                deadLetterQueueName,
                null,
                false,
                CancellationToken.None),
            Times.Once);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task DeclareAsync_WhenDeclarationFails_DisposesChannelAndPropagatesException()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var creator = CreateCreator(channelFactory);
        var declarationException =
            new InvalidOperationException("declaration failed");
        SetupChannel(channelFactory, channel);
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                "asyncomnia/deadletter",
                ExchangeType.Direct,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None))
            .ThrowsAsync(declarationException);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => creator.DeclareAsync(
            [
                CreateSubscription(
                    "events/clients/subscription/billing-service")
            ]));

        Assert.Same(declarationException, exception);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    private static RabbitMqDeadLetterTopologyCreator CreateCreator(
        Mock<IRabbitMqChannelFactory> channelFactory,
        bool deadLetteringEnabled = true)
    {
        return new RabbitMqDeadLetterTopologyCreator(
            channelFactory.Object,
            new RabbitMqDeadLetterRouteResolver(),
            Options.Create(
                new RabbitMqConsumerOptions
                {
                    EndpointName = "test-service",
                    DeadLetteringEnabled = deadLetteringEnabled
                }));
    }

    private static void SetupChannel(
        Mock<IRabbitMqChannelFactory> channelFactory,
        Mock<IChannel> channel)
    {
        channelFactory
            .Setup(x => x.CreateAsync(
                null,
                CancellationToken.None))
            .ReturnsAsync(channel.Object);
        channel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
    }

    private static void SetupExchangeDeclaration(
        Mock<IChannel> channel,
        string exchangeName)
    {
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                exchangeName,
                ExchangeType.Direct,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None))
            .Returns(Task.CompletedTask);
    }

    private static void SetupQueueDeclaration(
        Mock<IChannel> channel,
        string queueName,
        string exchangeName)
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
        channel
            .Setup(x => x.QueueBindAsync(
                queueName,
                exchangeName,
                queueName,
                null,
                false,
                CancellationToken.None))
            .Returns(Task.CompletedTask);
    }

    private static RabbitMqSubscription CreateSubscription(
        string queueName)
    {
        return new RabbitMqSubscription
        {
            ExchangeName = queueName.Split("/subscription/")[0],
            QueueName = queueName,
            ConsumerSettings = new RabbitMqConsumerSettings(1, 1, 1)
        };
    }
}
