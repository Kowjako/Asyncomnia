using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Infrastructure.Amqp.Topology;

public sealed class RabbitMqUnroutableTopologyCreatorTests
{
    [Fact]
    public async Task EnsureDeclaredAsync_DeclaresDurableFanoutExchangeQueueAndBinding()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        using var cancellation = new CancellationTokenSource();
        var token = cancellation.Token;
        SetupChannel(channelFactory, channel, token);
        SetupTopology(channel, token);
        var creator = new RabbitMqUnroutableTopologyCreator(
            channelFactory.Object);

        await creator.EnsureDeclaredAsync(token);

        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task EnsureDeclaredAsync_WhenCalledAgain_DoesNotDeclareAgain()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        SetupChannel(channelFactory, channel);
        SetupTopology(channel);
        var creator = new RabbitMqUnroutableTopologyCreator(
            channelFactory.Object);

        await creator.EnsureDeclaredAsync();
        await creator.EnsureDeclaredAsync();

        channelFactory.Verify(
            x => x.CreateAsync(null, CancellationToken.None),
            Times.Once);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task EnsureDeclaredAsync_WhenCalledConcurrently_DeclaresOnce()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var declarationStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseDeclaration = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        SetupChannel(channelFactory, channel);
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                RabbitMqUnroutableDefaults.ExchangeName,
                ExchangeType.Fanout,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None))
            .Returns(() =>
            {
                declarationStarted.TrySetResult();
                return releaseDeclaration.Task;
            });
        SetupQueueAndBinding(channel);
        var creator = new RabbitMqUnroutableTopologyCreator(
            channelFactory.Object);

        var first = creator.EnsureDeclaredAsync();
        await declarationStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var second = creator.EnsureDeclaredAsync();

        Assert.False(second.IsCompleted);
        releaseDeclaration.TrySetResult();
        await Task.WhenAll(first, second);

        channelFactory.Verify(
            x => x.CreateAsync(null, CancellationToken.None),
            Times.Once);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task EnsureDeclaredAsync_WhenDeclarationFails_RetriesOnNextCall()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var failingChannel = new Mock<IChannel>(MockBehavior.Strict);
        var retryChannel = new Mock<IChannel>(MockBehavior.Strict);
        var expectedException =
            new InvalidOperationException("declaration failed");
        channelFactory
            .SetupSequence(x => x.CreateAsync(
                null,
                CancellationToken.None))
            .ReturnsAsync(failingChannel.Object)
            .ReturnsAsync(retryChannel.Object);
        failingChannel
            .Setup(x => x.ExchangeDeclareAsync(
                RabbitMqUnroutableDefaults.ExchangeName,
                ExchangeType.Fanout,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None))
            .ThrowsAsync(expectedException);
        failingChannel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        retryChannel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        SetupTopology(retryChannel);
        var creator = new RabbitMqUnroutableTopologyCreator(
            channelFactory.Object);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => creator.EnsureDeclaredAsync());
        await creator.EnsureDeclaredAsync();

        Assert.Same(expectedException, exception);
        channelFactory.VerifyAll();
        failingChannel.VerifyAll();
        retryChannel.VerifyAll();
    }

    [Fact]
    public async Task EnsureDeclaredAsync_WhenWaitingCallerIsCancelled_DoesNotCancelActiveDeclaration()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var declarationStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseDeclaration = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        SetupChannel(channelFactory, channel);
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                RabbitMqUnroutableDefaults.ExchangeName,
                ExchangeType.Fanout,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None))
            .Returns(() =>
            {
                declarationStarted.TrySetResult();
                return releaseDeclaration.Task;
            });
        SetupQueueAndBinding(channel);
        var creator = new RabbitMqUnroutableTopologyCreator(
            channelFactory.Object);
        using var cancellation = new CancellationTokenSource();

        var active = creator.EnsureDeclaredAsync();
        await declarationStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var waiting = creator.EnsureDeclaredAsync(cancellation.Token);
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        Assert.False(active.IsCompleted);
        releaseDeclaration.TrySetResult();
        await active;

        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public void Constructor_WhenChannelFactoryIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new RabbitMqUnroutableTopologyCreator(null!));

        Assert.Equal("channelFactory", exception.ParamName);
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

    private static void SetupTopology(
        Mock<IChannel> channel,
        CancellationToken token = default)
    {
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                RabbitMqUnroutableDefaults.ExchangeName,
                ExchangeType.Fanout,
                true,
                false,
                null,
                false,
                false,
                token))
            .Returns(Task.CompletedTask);
        SetupQueueAndBinding(channel, token);
    }

    private static void SetupQueueAndBinding(
        Mock<IChannel> channel,
        CancellationToken token = default)
    {
        channel
            .Setup(x => x.QueueDeclareAsync(
                RabbitMqUnroutableDefaults.QueueName,
                true,
                false,
                false,
                null,
                false,
                false,
                token))
            .ReturnsAsync(new QueueDeclareOk(
                RabbitMqUnroutableDefaults.QueueName,
                0,
                0));
        channel
            .Setup(x => x.QueueBindAsync(
                RabbitMqUnroutableDefaults.QueueName,
                RabbitMqUnroutableDefaults.ExchangeName,
                string.Empty,
                null,
                false,
                token))
            .Returns(Task.CompletedTask);
    }
}
