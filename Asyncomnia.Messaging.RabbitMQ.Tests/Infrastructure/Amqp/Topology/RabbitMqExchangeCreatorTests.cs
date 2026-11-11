using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Infrastructure.Amqp.Topology;

public sealed class RabbitMqExchangeCreatorTests
{
    [Fact]
    public async Task EnsureDeclaredAsync_WithNewExchange_DeclaresDurableTopicExchangeAndDisposesChannel()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        using var cancellationSource = new CancellationTokenSource();
        var token = cancellationSource.Token;
        SetupChannelCreation(channelFactory, channel, token);
        SetupExchangeDeclaration(channel, "events/clients", token);
        var creator = new RabbitMqExchangeCreator(channelFactory.Object);

        await creator.EnsureDeclaredAsync("events/clients", token);

        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task EnsureDeclaredAsync_WhenExchangeWasAlreadyDeclared_DoesNotDeclareAgain()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        SetupChannelCreation(channelFactory, channel);
        SetupExchangeDeclaration(channel, "events/clients");
        var creator = new RabbitMqExchangeCreator(channelFactory.Object);

        await creator.EnsureDeclaredAsync("events/clients");
        await creator.EnsureDeclaredAsync("events/clients");

        channelFactory.Verify(
            x => x.CreateAsync(null, CancellationToken.None),
            Times.Once);
        channel.Verify(
            x => x.ExchangeDeclareAsync(
                "events/clients",
                ExchangeType.Topic,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None),
            Times.Once);
        channel.Verify(
            x => x.DisposeAsync(),
            Times.Once);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task EnsureDeclaredAsync_WhenSameExchangeIsRequestedConcurrently_DeclaresOnce()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var declarationStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseDeclaration = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        SetupChannelCreation(channelFactory, channel);
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                "events/clients",
                ExchangeType.Topic,
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
        var creator = new RabbitMqExchangeCreator(channelFactory.Object);

        var firstDeclaration =
            creator.EnsureDeclaredAsync("events/clients");
        await declarationStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(5));

        var secondDeclaration =
            creator.EnsureDeclaredAsync("events/clients");

        Assert.False(secondDeclaration.IsCompleted);

        releaseDeclaration.TrySetResult();
        await Task.WhenAll(firstDeclaration, secondDeclaration);

        channelFactory.Verify(
            x => x.CreateAsync(null, CancellationToken.None),
            Times.Once);
        channel.Verify(
            x => x.ExchangeDeclareAsync(
                "events/clients",
                ExchangeType.Topic,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None),
            Times.Once);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task EnsureDeclaredAsync_WhenDifferentExchangesAreRequested_DeclaresConcurrently()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var firstChannel = new Mock<IChannel>(MockBehavior.Strict);
        var secondChannel = new Mock<IChannel>(MockBehavior.Strict);
        var firstDeclarationStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstDeclaration = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        channelFactory
            .SetupSequence(x => x.CreateAsync(
                null,
                CancellationToken.None))
            .ReturnsAsync(firstChannel.Object)
            .ReturnsAsync(secondChannel.Object);
        firstChannel
            .Setup(x => x.ExchangeDeclareAsync(
                "events/clients",
                ExchangeType.Topic,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None))
            .Returns(() =>
            {
                firstDeclarationStarted.TrySetResult();
                return releaseFirstDeclaration.Task;
            });
        firstChannel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        SetupExchangeDeclaration(
            secondChannel,
            "commands/long-work-job");
        secondChannel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var creator = new RabbitMqExchangeCreator(channelFactory.Object);

        var firstDeclaration =
            creator.EnsureDeclaredAsync("events/clients");
        await firstDeclarationStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(5));

        var secondDeclaration =
            creator.EnsureDeclaredAsync("commands/long-work-job");
        await secondDeclaration.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.False(firstDeclaration.IsCompleted);

        releaseFirstDeclaration.TrySetResult();
        await firstDeclaration;

        channelFactory.Verify(
            x => x.CreateAsync(null, CancellationToken.None),
            Times.Exactly(2));
        channelFactory.VerifyAll();
        firstChannel.VerifyAll();
        secondChannel.VerifyAll();
    }

    [Fact]
    public async Task EnsureDeclaredAsync_WhenDeclarationFails_DoesNotCacheFailure()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var failingChannel = new Mock<IChannel>(MockBehavior.Strict);
        var retryChannel = new Mock<IChannel>(MockBehavior.Strict);
        var declarationException =
            new InvalidOperationException("declaration failed");
        channelFactory
            .SetupSequence(x => x.CreateAsync(
                null,
                CancellationToken.None))
            .ReturnsAsync(failingChannel.Object)
            .ReturnsAsync(retryChannel.Object);
        failingChannel
            .Setup(x => x.ExchangeDeclareAsync(
                "events/clients",
                ExchangeType.Topic,
                true,
                false,
                null,
                false,
                false,
                CancellationToken.None))
            .ThrowsAsync(declarationException);
        failingChannel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        SetupExchangeDeclaration(retryChannel, "events/clients");
        retryChannel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var creator = new RabbitMqExchangeCreator(channelFactory.Object);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => creator.EnsureDeclaredAsync("events/clients"));
        await creator.EnsureDeclaredAsync("events/clients");

        Assert.Same(declarationException, exception);
        channelFactory.Verify(
            x => x.CreateAsync(null, CancellationToken.None),
            Times.Exactly(2));
        channelFactory.VerifyAll();
        failingChannel.VerifyAll();
        retryChannel.VerifyAll();
    }

    [Fact]
    public async Task EnsureDeclaredAsync_WhenWaitingCallerIsCancelled_CancelsOnlyWaitingCaller()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var declarationStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseDeclaration = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        SetupChannelCreation(channelFactory, channel);
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                "events/clients",
                ExchangeType.Topic,
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
        var creator = new RabbitMqExchangeCreator(channelFactory.Object);
        using var cancellationSource = new CancellationTokenSource();

        var activeDeclaration =
            creator.EnsureDeclaredAsync("events/clients");
        await declarationStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(5));
        var waitingDeclaration = creator.EnsureDeclaredAsync(
            "events/clients",
            cancellationSource.Token);

        cancellationSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => waitingDeclaration);
        Assert.False(activeDeclaration.IsCompleted);

        releaseDeclaration.TrySetResult();
        await activeDeclaration;
        await creator.EnsureDeclaredAsync("events/clients");

        channelFactory.Verify(
            x => x.CreateAsync(null, CancellationToken.None),
            Times.Once);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task EnsureDeclaredAsync_WithInvalidExchangeName_Throws(
        string? exchangeName)
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var creator = new RabbitMqExchangeCreator(channelFactory.Object);

        await Assert.ThrowsAnyAsync<ArgumentException>(
            () => creator.EnsureDeclaredAsync(exchangeName!));

        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public void Constructor_WhenChannelFactoryIsNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => new RabbitMqExchangeCreator(null!));
    }

    private static void SetupChannelCreation(
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
        CancellationToken token = default)
    {
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                exchangeName,
                ExchangeType.Topic,
                true,
                false,
                null,
                false,
                false,
                token))
            .Returns(Task.CompletedTask);
    }
}
