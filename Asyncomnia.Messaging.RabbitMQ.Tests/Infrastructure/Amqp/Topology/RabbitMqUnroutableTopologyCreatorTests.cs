using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;
using Microsoft.Extensions.Options;
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
        var creator = CreateCreator(channelFactory);

        await creator.EnsureDeclaredAsync(token);

        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task EnsureDeclaredAsync_UsesConfiguredQueueLimitsAndDropHeadOverflow()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var options = new RabbitMqOptions
        {
            DefaultQueueType = RabbitMqQueueType.Classic,
            UnroutableQueueMaxLength = 25,
            UnroutableQueueMaxLengthBytes = 4096
        };
        SetupChannel(channelFactory, channel);
        SetupTopology(channel, options: options);
        var creator = CreateCreator(channelFactory, options);

        await creator.EnsureDeclaredAsync();

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
        var creator = CreateCreator(channelFactory);

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
        var creator = CreateCreator(channelFactory);

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
        var creator = CreateCreator(channelFactory);

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
        var creator = CreateCreator(channelFactory);
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
            new RabbitMqUnroutableTopologyCreator(
                null!,
                Options.Create(new RabbitMqOptions())));

        Assert.Equal("channelFactory", exception.ParamName);
    }

    [Fact]
    public void Constructor_WhenOptionsAreNull_ThrowsArgumentNullException()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);

        var exception = Assert.Throws<ArgumentNullException>(() =>
            new RabbitMqUnroutableTopologyCreator(
                channelFactory.Object,
                null!));

        Assert.Equal("rabbitMqOptions", exception.ParamName);
    }

    private static RabbitMqUnroutableTopologyCreator CreateCreator(
        Mock<IRabbitMqChannelFactory> channelFactory,
        RabbitMqOptions? options = null)
    {
        return new RabbitMqUnroutableTopologyCreator(
            channelFactory.Object,
            Options.Create(options ?? new RabbitMqOptions()));
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
        CancellationToken token = default,
        RabbitMqOptions? options = null)
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
        SetupQueueAndBinding(channel, token, options);
    }

    private static void SetupQueueAndBinding(
        Mock<IChannel> channel,
        CancellationToken token = default,
        RabbitMqOptions? options = null)
    {
        options ??= new RabbitMqOptions();

        channel
            .Setup(x => x.QueueDeclareAsync(
                RabbitMqUnroutableDefaults.QueueName,
                true,
                false,
                false,
                It.Is<IDictionary<string, object?>?>(
                    arguments => HasQueueArguments(arguments, options)),
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

    private static bool HasQueueArguments(
        IDictionary<string, object?>? arguments,
        RabbitMqOptions options)
    {
        var expectedQueueType = options.DefaultQueueType ==
                                RabbitMqQueueType.Quorum
            ? "quorum"
            : "classic";

        return arguments is not null &&
               arguments.Count == 4 &&
               arguments.TryGetValue(
                   "x-queue-type",
                   out var queueType) &&
               Equals(queueType, expectedQueueType) &&
               arguments.TryGetValue("x-max-length", out var maxLength) &&
               Equals(maxLength, options.UnroutableQueueMaxLength) &&
               arguments.TryGetValue(
                   "x-max-length-bytes",
                   out var maxLengthBytes) &&
               Equals(maxLengthBytes, options.UnroutableQueueMaxLengthBytes) &&
               arguments.TryGetValue("x-overflow", out var overflow) &&
               Equals(overflow, "drop-head");
    }
}
