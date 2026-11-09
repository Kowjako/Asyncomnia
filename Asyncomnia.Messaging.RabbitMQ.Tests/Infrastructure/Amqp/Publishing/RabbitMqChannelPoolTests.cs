using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Infrastructure.Amqp.Publishing;

public sealed class RabbitMqChannelPoolTests
{
    [Fact]
    public async Task Constructor_DoesNotCreateChannelsUntilLeasedSlotPublishes()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channelPool = CreatePool(
            channelFactory,
            publisherChannelCount: 4);

        var slot = await channelPool.RentAsync();
        channelPool.Return(slot);
        await channelPool.DisposeAsync();

        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task RentAsync_WhenAllSlotsAreRented_WaitsUntilSlotIsReturned()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channelPool = CreatePool(
            channelFactory,
            publisherChannelCount: 2);
        var firstSlot = await channelPool.RentAsync();
        var secondSlot = await channelPool.RentAsync();

        var waitingRent = channelPool.RentAsync().AsTask();

        Assert.False(waitingRent.IsCompleted);

        channelPool.Return(firstSlot);
        var returnedSlot = await waitingRent.WaitAsync(
            TimeSpan.FromSeconds(5));

        Assert.Same(firstSlot, returnedSlot);

        channelPool.Return(secondSlot);
        channelPool.Return(returnedSlot);
        await channelPool.DisposeAsync();
        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task DisposeAsync_WhenSlotIsPublishing_WaitsAndRejectsWaitingRent()
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
        channelFactory
            .Setup(x => x.CreateAsync(
                It.Is<CreateChannelOptions>(options =>
                    HasPublisherConfirmations(options)),
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
            .Returns(() =>
            {
                publicationStarted.TrySetResult();
                return new ValueTask(releasePublication.Task);
            });
        channel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var channelPool = CreatePool(
            channelFactory,
            publisherChannelCount: 1);
        var slot = await channelPool.RentAsync();
        var publication = slot.PublishAsync(
            "events/clients",
            "client-created-event",
            false,
            properties,
            body,
            CancellationToken.None);
        await publicationStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(5));
        var waitingRent = channelPool.RentAsync().AsTask();

        var disposal = channelPool.DisposeAsync().AsTask();

        Assert.False(disposal.IsCompleted);
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => waitingRent);

        releasePublication.TrySetResult();
        await publication;
        await disposal;

        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task RentAsync_AfterPoolWasDisposed_Throws()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channelPool = CreatePool(
            channelFactory,
            publisherChannelCount: 1);
        await channelPool.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            async () => await channelPool.RentAsync());

        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Return_AfterPoolWasDisposed_IsIgnoredAndDisposeRemainsIdempotent()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channelPool = CreatePool(
            channelFactory,
            publisherChannelCount: 1);
        var slot = await channelPool.RentAsync();
        await channelPool.DisposeAsync();

        channelPool.Return(slot);
        await channelPool.DisposeAsync();

        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Return_WhenSlotIsNull_Throws()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channelPool = CreatePool(
            channelFactory,
            publisherChannelCount: 1);

        Assert.Throws<ArgumentNullException>(
            () => channelPool.Return(null!));

        await channelPool.DisposeAsync();
        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public void Constructor_WhenChannelFactoryIsNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => new RabbitMqChannelPool(
                null!,
                Options.Create(new RabbitMqOptions())));
    }

    [Fact]
    public void Constructor_WhenOptionsAreNull_Throws()
    {
        var channelFactory = new Mock<IRabbitMqChannelFactory>();

        Assert.Throws<ArgumentNullException>(
            () => new RabbitMqChannelPool(
                channelFactory.Object,
                null!));
    }

    private static RabbitMqChannelPool CreatePool(Mock<IRabbitMqChannelFactory> channelFactory, int publisherChannelCount)
    {
        return new RabbitMqChannelPool(
            channelFactory.Object,
            Options.Create(
                new RabbitMqOptions
                {
                    PublisherChannelCount = publisherChannelCount
                }));
    }

    private static bool HasPublisherConfirmations(CreateChannelOptions options)
    {
        return options.PublisherConfirmationsEnabled
            && options.PublisherConfirmationTrackingEnabled;
    }
}
