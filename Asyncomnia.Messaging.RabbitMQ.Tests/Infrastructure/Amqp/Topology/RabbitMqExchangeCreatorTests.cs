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
        var creator = CreateCreator(channelFactory);

        await creator.EnsureDeclaredAsync(
            "events/clients",
            useUnroutableFallback: false,
            token: token);

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
        var creator = CreateCreator(channelFactory);

        await creator.EnsureDeclaredAsync("events/clients", false);
        await creator.EnsureDeclaredAsync("events/clients", false);

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
        var creator = CreateCreator(channelFactory);

        var firstDeclaration =
            creator.EnsureDeclaredAsync("events/clients", false);
        await declarationStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(5));

        var secondDeclaration =
            creator.EnsureDeclaredAsync("events/clients", false);

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
        var creator = CreateCreator(channelFactory);

        var firstDeclaration =
            creator.EnsureDeclaredAsync("events/clients", false);
        await firstDeclarationStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(5));

        var secondDeclaration =
            creator.EnsureDeclaredAsync(
                "commands/long-work-job",
                false);
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
        var creator = CreateCreator(channelFactory);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => creator.EnsureDeclaredAsync("events/clients", false));
        await creator.EnsureDeclaredAsync("events/clients", false);

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
        var creator = CreateCreator(channelFactory);
        using var cancellationSource = new CancellationTokenSource();

        var activeDeclaration =
            creator.EnsureDeclaredAsync("events/clients", false);
        await declarationStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(5));
        var waitingDeclaration = creator.EnsureDeclaredAsync(
            "events/clients",
            false,
            cancellationSource.Token);

        cancellationSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => waitingDeclaration);
        Assert.False(activeDeclaration.IsCompleted);

        releaseDeclaration.TrySetResult();
        await activeDeclaration;
        await creator.EnsureDeclaredAsync("events/clients", false);

        channelFactory.Verify(
            x => x.CreateAsync(null, CancellationToken.None),
            Times.Once);
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task EnsureDeclaredAsync_WithUnroutableFallback_DeclaresFallbackBeforeExchange()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var unroutableTopologyCreator =
            new Mock<IRabbitMqUnroutableTopologyCreator>(MockBehavior.Strict);
        var sequence = new MockSequence();
        unroutableTopologyCreator
            .InSequence(sequence)
            .Setup(x => x.EnsureDeclaredAsync(CancellationToken.None))
            .Returns(Task.CompletedTask);
        channelFactory
            .InSequence(sequence)
            .Setup(x => x.CreateAsync(null, CancellationToken.None))
            .ReturnsAsync(channel.Object);
        channel
            .InSequence(sequence)
            .Setup(x => x.ExchangeDeclareAsync(
                "events/clients",
                ExchangeType.Topic,
                true,
                false,
                It.Is<IDictionary<string, object?>?>(
                    arguments => HasUnroutableFallback(arguments)),
                false,
                false,
                CancellationToken.None))
            .Returns(Task.CompletedTask);
        channel
            .Setup(x => x.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var creator = CreateCreator(
            channelFactory,
            unroutableTopologyCreator);

        await creator.EnsureDeclaredAsync(
            "events/clients",
            useUnroutableFallback: true);

        unroutableTopologyCreator.VerifyAll();
        channelFactory.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task EnsureDeclaredAsync_WhenUnroutableTopologyFails_DoesNotCreateExchangeChannel()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var unroutableTopologyCreator =
            new Mock<IRabbitMqUnroutableTopologyCreator>(MockBehavior.Strict);
        var expectedException =
            new InvalidOperationException("unroutable topology failed");
        unroutableTopologyCreator
            .Setup(x => x.EnsureDeclaredAsync(CancellationToken.None))
            .ThrowsAsync(expectedException);
        var creator = CreateCreator(
            channelFactory,
            unroutableTopologyCreator);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => creator.EnsureDeclaredAsync("events/clients", true));

        Assert.Same(expectedException, exception);
        channelFactory.VerifyNoOtherCalls();
        unroutableTopologyCreator.VerifyAll();
    }

    [Fact]
    public async Task EnsureDeclaredAsync_WhenCachedConfigurationDiffers_ThrowsWithoutRedeclaring()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var unroutableTopologyCreator =
            new Mock<IRabbitMqUnroutableTopologyCreator>(MockBehavior.Strict);
        unroutableTopologyCreator
            .Setup(x => x.EnsureDeclaredAsync(CancellationToken.None))
            .Returns(Task.CompletedTask);
        SetupChannelCreation(channelFactory, channel);
        channel
            .Setup(x => x.ExchangeDeclareAsync(
                "events/clients",
                ExchangeType.Topic,
                true,
                false,
                It.Is<IDictionary<string, object?>?>(
                    arguments => HasUnroutableFallback(arguments)),
                false,
                false,
                CancellationToken.None))
            .Returns(Task.CompletedTask);
        var creator = CreateCreator(
            channelFactory,
            unroutableTopologyCreator);
        await creator.EnsureDeclaredAsync("events/clients", true);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => creator.EnsureDeclaredAsync("events/clients", false));

        Assert.Contains("different unroutable fallback", exception.Message);
        channelFactory.Verify(
            x => x.CreateAsync(null, CancellationToken.None),
            Times.Once);
        unroutableTopologyCreator.VerifyAll();
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
        var creator = CreateCreator(channelFactory);

        await Assert.ThrowsAnyAsync<ArgumentException>(
            () => creator.EnsureDeclaredAsync(exchangeName!, false));

        channelFactory.VerifyNoOtherCalls();
    }

    [Fact]
    public void Constructor_WhenChannelFactoryIsNull_Throws()
    {
        var unroutableTopologyCreator =
            new Mock<IRabbitMqUnroutableTopologyCreator>(MockBehavior.Strict);

        Assert.Throws<ArgumentNullException>(
            () => new RabbitMqExchangeCreator(
                null!,
                unroutableTopologyCreator.Object));
    }

    [Fact]
    public void Constructor_WhenUnroutableTopologyCreatorIsNull_Throws()
    {
        var channelFactory =
            new Mock<IRabbitMqChannelFactory>(MockBehavior.Strict);

        Assert.Throws<ArgumentNullException>(
            () => new RabbitMqExchangeCreator(
                channelFactory.Object,
                null!));
    }

    private static RabbitMqExchangeCreator CreateCreator(
        Mock<IRabbitMqChannelFactory> channelFactory,
        Mock<IRabbitMqUnroutableTopologyCreator>? unroutableTopologyCreator = null)
    {
        unroutableTopologyCreator ??=
            new Mock<IRabbitMqUnroutableTopologyCreator>(MockBehavior.Strict);

        return new RabbitMqExchangeCreator(
            channelFactory.Object,
            unroutableTopologyCreator.Object);
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

    private static bool HasUnroutableFallback(
        IDictionary<string, object?>? arguments)
    {
        return arguments is not null &&
               arguments.Count == 1 &&
               arguments.TryGetValue("alternate-exchange", out var value) &&
               Equals(value, RabbitMqUnroutableDefaults.ExchangeName);
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
