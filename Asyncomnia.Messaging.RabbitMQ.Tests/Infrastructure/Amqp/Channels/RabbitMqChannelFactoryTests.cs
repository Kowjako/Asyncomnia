using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Connection;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Infrastructure.Amqp.Channels;

public sealed class RabbitMqChannelFactoryTests
{
    [Fact]
    public async Task CreateAsync_ForwardsOptionsAndCancellationToken()
    {
        var connectionProvider =
            new Mock<IRabbitMqConnectionProvider>(MockBehavior.Strict);
        var connection = new Mock<IConnection>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var options = new CreateChannelOptions(
            publisherConfirmationsEnabled: false,
            publisherConfirmationTrackingEnabled: false,
            consumerDispatchConcurrency: 3);
        using var cancellationTokenSource =
            new CancellationTokenSource();
        var token = cancellationTokenSource.Token;
        connectionProvider
            .Setup(x => x.GetConnectionAsync(token))
            .Returns(new ValueTask<IConnection>(connection.Object));
        connection
            .Setup(x => x.CreateChannelAsync(options, token))
            .ReturnsAsync(channel.Object);
        var factory =
            new RabbitMqChannelFactory(connectionProvider.Object);

        var createdChannel = await factory.CreateAsync(options, token);

        Assert.Same(channel.Object, createdChannel);
        connectionProvider.VerifyAll();
        connection.VerifyAll();
    }

    [Fact]
    public async Task CreateAsync_WithoutOptions_CreatesDefaultChannel()
    {
        var connectionProvider =
            new Mock<IRabbitMqConnectionProvider>(MockBehavior.Strict);
        var connection = new Mock<IConnection>(MockBehavior.Strict);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        connectionProvider
            .Setup(x => x.GetConnectionAsync(CancellationToken.None))
            .Returns(new ValueTask<IConnection>(connection.Object));
        connection
            .Setup(x => x.CreateChannelAsync(
                null,
                CancellationToken.None))
            .ReturnsAsync(channel.Object);
        var factory =
            new RabbitMqChannelFactory(connectionProvider.Object);

        var createdChannel = await factory.CreateAsync();

        Assert.Same(channel.Object, createdChannel);
        connectionProvider.VerifyAll();
        connection.VerifyAll();
    }

    [Fact]
    public void Constructor_WhenConnectionProviderIsNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => new RabbitMqChannelFactory(null!));
    }
}
