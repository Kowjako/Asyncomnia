using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Unhandled;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.Unhandled;

public sealed class RabbitMqUnhandledTopologyCreatorTests
{
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
    public async Task DeclareAsync_WithSubscriptions_DeclaresExchangeAndQueues()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var creator = CreateCreator(
            channelFactory,
            RabbitMqQueueType.Classic);
        var sourceQueueNames = new[]
        {
            "commands/jobs/subscription/worker-service",
            "events/clients/subscription/worker-service"
        };
        var unhandledQueueNames = sourceQueueNames
            .Select(queue => $"unhandled/{queue}")
            .ToArray();
        SetupChannel(channelFactory, channel);
        SetupExchange(channel);

        foreach (var queueName in unhandledQueueNames)
        {
            SetupQueue(channel, queueName, RabbitMqQueueType.Classic);
        }

        await creator.DeclareAsync(
            sourceQueueNames.Select(CreateSubscription).ToArray());

        channel.Verify(
            x => x.ExchangeDeclareAsync(
                "asyncomnia/unhandled",
                ExchangeType.Direct,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None),
            Times.Once);

        foreach (var queueName in unhandledQueueNames)
        {
            channel.Verify(
                x => x.QueueDeclareAsync(
                    queueName,
                    true,
                    false,
                    false,
                    It.Is<IDictionary<string, object?>?>(
                        arguments => HasQueueType(
                            arguments,
                            RabbitMqQueueType.Classic)),
                    false,
                    false,
                    CancellationToken.None),
                Times.Once);
            channel.Verify(
                x => x.QueueBindAsync(
                    queueName,
                    "asyncomnia/unhandled",
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
    public async Task DeclareAsync_WithDuplicateSourceQueues_DeclaresQueueOnce()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var creator = CreateCreator(channelFactory);
        const string sourceQueueName =
            "events/clients/subscription/billing-service";
        const string unhandledQueueName =
            "unhandled/events/clients/subscription/billing-service";
        SetupChannel(channelFactory, channel);
        SetupExchange(channel);
        SetupQueue(channel, unhandledQueueName, RabbitMqQueueType.Quorum);

        await creator.DeclareAsync(
        [
            CreateSubscription(sourceQueueName),
            CreateSubscription(sourceQueueName)
        ]);

        channel.Verify(
            x => x.QueueDeclareAsync(
                unhandledQueueName,
                true,
                false,
                false,
                It.IsAny<IDictionary<string, object?>?>(),
                false,
                false,
                CancellationToken.None),
            Times.Once);
        channel.Verify(
            x => x.QueueBindAsync(
                unhandledQueueName,
                "asyncomnia/unhandled",
                unhandledQueueName,
                null,
                false,
                CancellationToken.None),
            Times.Once);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task DeclareAsync_WhenDeclarationFails_PropagatesException()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var creator = CreateCreator(channelFactory);
        var expected = new InvalidOperationException("declaration failed");
        SetupChannel(channelFactory, channel);
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                "asyncomnia/unhandled",
                ExchangeType.Direct,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None))
            .ThrowsAsync(expected);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => creator.DeclareAsync(
            [
                CreateSubscription(
                    "events/clients/subscription/billing-service")
            ]));

        Assert.Same(expected, exception);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    private static RabbitMqUnhandledTopologyCreator CreateCreator(
        Mock<IRabbitMqChannelFactory> channelFactory,
        RabbitMqQueueType queueType = RabbitMqQueueType.Quorum)
    {
        return new RabbitMqUnhandledTopologyCreator(
            channelFactory.Object,
            new RabbitMqUnhandledRouteResolver(),
            Options.Create(new RabbitMqConsumerOptions
            {
                EndpointName = "test-service",
                DefaultQueueType = queueType
            }));
    }

    private static void SetupChannel(
        Mock<IRabbitMqChannelFactory> channelFactory,
        Mock<IChannel> channel)
    {
        channelFactory
            .Setup(x => x.CreateAsync(null, CancellationToken.None))
            .ReturnsAsync(channel.Object);
        channel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
    }

    private static void SetupExchange(Mock<IChannel> channel)
    {
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                "asyncomnia/unhandled",
                ExchangeType.Direct,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None))
            .Returns(Task.CompletedTask);
    }

    private static void SetupQueue(
        Mock<IChannel> channel,
        string queueName,
        RabbitMqQueueType queueType)
    {
        channel
            .Setup(x => x.QueueDeclareAsync(
                queueName,
                true,
                false,
                false,
                It.Is<IDictionary<string, object?>?>(
                    arguments => HasQueueType(arguments, queueType)),
                false,
                false,
                CancellationToken.None))
            .ReturnsAsync(new QueueDeclareOk(queueName, 0, 0));
        channel
            .Setup(x => x.QueueBindAsync(
                queueName,
                "asyncomnia/unhandled",
                queueName,
                null,
                false,
                CancellationToken.None))
            .Returns(Task.CompletedTask);
    }

    private static RabbitMqSubscription CreateSubscription(string queueName)
    {
        return new RabbitMqSubscription
        {
            ExchangeName = queueName.Split("/subscription/")[0],
            QueueName = queueName,
            Settings = new RabbitMqSubscriptionSettings(
                RabbitMqQueueType.Quorum,
                false,
                1,
                1)
        };
    }

    private static bool HasQueueType(
        IDictionary<string, object?>? arguments,
        RabbitMqQueueType queueType)
    {
        var expected = queueType == RabbitMqQueueType.Quorum
            ? "quorum"
            : "classic";

        return arguments is not null &&
               arguments.Count == 1 &&
               arguments.TryGetValue("x-queue-type", out var value) &&
               Equals(value, expected);
    }
}
