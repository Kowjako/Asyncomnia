using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Infrastructure.Amqp.Publishing;

public sealed class RabbitMqChannelPoolSlotTests
{
    [Fact]
    public async Task PublishAsync_WhenChannelWasNotCreated_CreatesConfirmingChannelAndPublishesMessage()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        using var cancellationTokenSource =
            new CancellationTokenSource();
        var token = cancellationTokenSource.Token;
        var properties = new BasicProperties();
        ReadOnlyMemory<byte> body = new byte[] { 1, 2, 3 };
        SetupChannelCreation(channelFactory, channel, token);
        SetupPublication(
            channel,
            "events/clients",
            "client-created-event",
            false,
            properties,
            body,
            token);
        channel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var slot = new RabbitMqChannelPoolSlot(channelFactory.Object);

        await slot.PublishAsync(
            "events/clients",
            "client-created-event",
            false,
            properties,
            body,
            token);
        await slot.DisposeAsync();

        channelFactory.Verify(
            x => x.CreateAsync(
                It.Is<CreateChannelOptions>(options =>
                    HasPublisherConfirmations(options)),
                token),
            Times.Once);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task PublishAsync_WhenChannelIsOpen_ReusesChannel()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var properties = new BasicProperties();
        ReadOnlyMemory<byte> body = new byte[] { 1 };
        SetupChannelCreation(channelFactory, channel);
        channel
            .SetupGet(x => x.IsOpen)
            .Returns(true);
        channel
            .Setup(x => x.BasicPublishAsync(
                "events/clients",
                "client-created-event",
                false,
                properties,
                body,
                CancellationToken.None))
            .Returns(ValueTask.CompletedTask);
        channel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var slot = new RabbitMqChannelPoolSlot(channelFactory.Object);

        await slot.PublishAsync(
            "events/clients",
            "client-created-event",
            false,
            properties,
            body,
            CancellationToken.None);
        await slot.PublishAsync(
            "events/clients",
            "client-created-event",
            false,
            properties,
            body,
            CancellationToken.None);
        await slot.DisposeAsync();

        channelFactory.Verify(
            x => x.CreateAsync(
                It.IsAny<CreateChannelOptions>(),
                CancellationToken.None),
            Times.Once);
        channel.Verify(
            x => x.BasicPublishAsync(
                "events/clients",
                "client-created-event",
                false,
                properties,
                body,
                CancellationToken.None),
            Times.Exactly(2));
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task PublishAsync_WhenChannelIsClosed_DisposesAndReplacesChannel()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var closedChannel = new Mock<IChannel>(MockBehavior.Strict);
        var replacementChannel =
            new Mock<IChannel>(MockBehavior.Strict);
        var properties = new BasicProperties();
        ReadOnlyMemory<byte> body = new byte[] { 1 };
        channelFactory
            .SetupSequence(x => x.CreateAsync(
                It.Is<CreateChannelOptions>(options =>
                    HasPublisherConfirmations(options)),
                CancellationToken.None))
            .ReturnsAsync(closedChannel.Object)
            .ReturnsAsync(replacementChannel.Object);
        SetupPublication(
            closedChannel,
            "events/clients",
            "client-created-event",
            false,
            properties,
            body);
        closedChannel
            .SetupGet(x => x.IsOpen)
            .Returns(false);
        closedChannel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        SetupPublication(
            replacementChannel,
            "events/clients",
            "client-created-event",
            false,
            properties,
            body);
        replacementChannel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var slot = new RabbitMqChannelPoolSlot(channelFactory.Object);

        await slot.PublishAsync(
            "events/clients",
            "client-created-event",
            false,
            properties,
            body,
            CancellationToken.None);
        await slot.PublishAsync(
            "events/clients",
            "client-created-event",
            false,
            properties,
            body,
            CancellationToken.None);
        await slot.DisposeAsync();

        channelFactory.Verify(
            x => x.CreateAsync(
                It.IsAny<CreateChannelOptions>(),
                CancellationToken.None),
            Times.Exactly(2));
        closedChannel.Verify(
            x => x.DisposeAsync(),
            Times.Once);
        replacementChannel.Verify(
            x => x.DisposeAsync(),
            Times.Once);
        channelFactory.VerifyAll();
        closedChannel.VerifyAll();
        replacementChannel.VerifyAll();
    }

    [Fact]
    public async Task DisposeAsync_WhenPublicationIsRunning_WaitsAndThenDisposesChannel()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var publicationStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePublication = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var properties = new BasicProperties();
        ReadOnlyMemory<byte> body = new byte[] { 1 };
        SetupChannelCreation(channelFactory, channel);
        channel
            .Setup(x => x.BasicPublishAsync(
                "events/clients",
                "client-created-event",
                false,
                properties,
                body,
                CancellationToken.None))
            .Returns(() =>
            {
                publicationStarted.TrySetResult();
                return new ValueTask(releasePublication.Task);
            });
        channel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var slot = new RabbitMqChannelPoolSlot(channelFactory.Object);

        var publication = slot.PublishAsync(
            "events/clients",
            "client-created-event",
            false,
            properties,
            body,
            CancellationToken.None);
        await publicationStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(5));

        var disposal = slot.DisposeAsync().AsTask();

        Assert.False(disposal.IsCompleted);

        releasePublication.TrySetResult();
        await publication;
        await disposal;

        channel.Verify(
            x => x.DisposeAsync(),
            Times.Once);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task PublishAsync_AfterSlotWasDisposed_ThrowsWithoutCreatingChannel()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var slot = new RabbitMqChannelPoolSlot(channelFactory.Object);
        await slot.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => slot.PublishAsync(
                "events/clients",
                "client-created-event",
                false,
                new BasicProperties(),
                ReadOnlyMemory<byte>.Empty,
                CancellationToken.None));

        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public void Constructor_WhenChannelFactoryIsNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => new RabbitMqChannelPoolSlot(null!));
    }

    private static void SetupChannelCreation(Mock<IRabbitMqChannelFactory> channelFactory, Mock<IChannel> channel, CancellationToken token = default)
    {
        channelFactory
            .Setup(x => x.CreateAsync(
                It.Is<CreateChannelOptions>(options =>
                    HasPublisherConfirmations(options)),
                token))
            .ReturnsAsync(channel.Object);
    }

    private static void SetupPublication(
        Mock<IChannel> channel,
        string exchange,
        string routingKey,
        bool mandatory,
        BasicProperties properties,
        ReadOnlyMemory<byte> body,
        CancellationToken token = default)
    {
        channel
            .Setup(x => x.BasicPublishAsync(
                exchange,
                routingKey,
                mandatory,
                properties,
                body,
                token))
            .Returns(ValueTask.CompletedTask);
    }

    private static bool HasPublisherConfirmations(CreateChannelOptions options)
    {
        return options.PublisherConfirmationsEnabled
            && options.PublisherConfirmationTrackingEnabled;
    }
}
