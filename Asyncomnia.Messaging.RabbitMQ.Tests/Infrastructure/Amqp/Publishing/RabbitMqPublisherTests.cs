using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Infrastructure.Amqp.Publishing;

public sealed class RabbitMqPublisherTests
{
    [Fact]
    public async Task PublishAsync_RentsSlotPublishesMessageAndReturnsSlot()
    {
        var channelPool =
            new Mock<IRabbitMqChannelPool>(MockBehavior.Strict);
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        using var cancellationTokenSource =
            new CancellationTokenSource();
        var token = cancellationTokenSource.Token;
        var properties = new BasicProperties();
        ReadOnlyMemory<byte> body = new byte[] { 1, 2, 3 };
        var slot = new RabbitMqChannelPoolSlot(
            channelFactory.Object);
        channelPool
            .Setup(x => x.RentAsync(token))
            .Returns(new ValueTask<RabbitMqChannelPoolSlot>(slot));
        channelFactory
            .Setup(x => x.CreateAsync(
                It.Is<CreateChannelOptions>(options =>
                    HasPublisherConfirmations(options)),
                token))
            .ReturnsAsync(channel.Object);
        channel
            .Setup(x => x.BasicPublishAsync(
                "events/clients",
                "client-created-event",
                false,
                properties,
                body,
                token))
            .Returns(ValueTask.CompletedTask);
        channelPool
            .Setup(x => x.Return(slot));
        channel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var publisher = new RabbitMqPublisher(channelPool.Object);

        await publisher.PublishAsync(
            "events/clients",
            "client-created-event",
            false,
            properties,
            body,
            token);
        await slot.DisposeAsync();

        channelPool.VerifyAll();
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task PublishAsync_WhenSlotPublicationFails_ReturnsSlotAndPropagatesException()
    {
        var channelPool =
            new Mock<IRabbitMqChannelPool>(MockBehavior.Strict);
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var publicationException =
            new InvalidOperationException("publication failed");
        var properties = new BasicProperties();
        ReadOnlyMemory<byte> body = new byte[] { 1 };
        var slot = new RabbitMqChannelPoolSlot(
            channelFactory.Object);
        channelPool
            .Setup(x => x.RentAsync(CancellationToken.None))
            .Returns(new ValueTask<RabbitMqChannelPoolSlot>(slot));
        channelFactory
            .Setup(x => x.CreateAsync(
                It.IsAny<CreateChannelOptions>(),
                CancellationToken.None))
            .ReturnsAsync(channel.Object);
        channel
            .Setup(x => x.BasicPublishAsync(
                "events/clients",
                "client-created-event",
                false,
                properties,
                body,
                CancellationToken.None))
            .Returns(new ValueTask(
                Task.FromException(publicationException)));
        channelPool
            .Setup(x => x.Return(slot));
        channel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var publisher = new RabbitMqPublisher(channelPool.Object);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => publisher.PublishAsync(
                "events/clients",
                "client-created-event",
                false,
                properties,
                body));
        await slot.DisposeAsync();

        Assert.Same(publicationException, exception);
        channelPool.Verify(
            x => x.Return(slot),
            Times.Once);
        channelPool.VerifyAll();
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task PublishAsync_WhenRentFails_DoesNotReturnSlotAndPropagatesException()
    {
        var channelPool =
            new Mock<IRabbitMqChannelPool>(MockBehavior.Strict);
        var rentException =
            new InvalidOperationException("rent failed");
        channelPool
            .Setup(x => x.RentAsync(CancellationToken.None))
            .Returns(new ValueTask<RabbitMqChannelPoolSlot>(
                Task.FromException<RabbitMqChannelPoolSlot>(
                    rentException)));
        var publisher = new RabbitMqPublisher(channelPool.Object);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => publisher.PublishAsync(
                "events/clients",
                "client-created-event",
                false,
                new BasicProperties(),
                ReadOnlyMemory<byte>.Empty));

        Assert.Same(rentException, exception);
        channelPool.VerifyAll();
    }

    [Fact]
    public async Task PublishAsync_WithNullArguments_ThrowsBeforeRentingSlot()
    {
        var channelPool =
            new Mock<IRabbitMqChannelPool>(MockBehavior.Strict);
        var publisher = new RabbitMqPublisher(channelPool.Object);
        var properties = new BasicProperties();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => publisher.PublishAsync(
                null!,
                "routing-key",
                false,
                properties,
                ReadOnlyMemory<byte>.Empty));
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => publisher.PublishAsync(
                "exchange",
                null!,
                false,
                properties,
                ReadOnlyMemory<byte>.Empty));
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => publisher.PublishAsync(
                "exchange",
                "routing-key",
                false,
                null!,
                ReadOnlyMemory<byte>.Empty));

        channelPool.VerifyNoOtherCalls();
    }

    [Fact]
    public void Constructor_WhenChannelPoolIsNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => new RabbitMqPublisher(null!));
    }

    private static bool HasPublisherConfirmations(
        CreateChannelOptions options)
    {
        return options.PublisherConfirmationsEnabled
            && options.PublisherConfirmationTrackingEnabled;
    }
}
