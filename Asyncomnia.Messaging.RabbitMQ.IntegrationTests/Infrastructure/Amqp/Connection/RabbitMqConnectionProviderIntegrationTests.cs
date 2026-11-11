using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Connection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.IntegrationTests.Infrastructure.Amqp.Connection;

[Collection(RabbitMqCollection.Name)]
public sealed class RabbitMqConnectionProviderIntegrationTests(
    RabbitMqContainerFixture fixture)
{
    [Fact]
    public async Task GetConnectionAsync_WhenCalledConcurrently_ReturnsSameOpenConnection()
    {
        await using var provider = CreateProvider();
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = cancellationTokenSource.Token;

        var connectionTasks = Enumerable
            .Range(0, 8)
            .Select(async _ => await provider.GetConnectionAsync(token))
            .ToArray();

        var connections = await Task.WhenAll(connectionTasks);

        var connection = connections[0];
        Assert.True(connection.IsOpen);
        Assert.All(
            connections,
            returnedConnection => Assert.Same(
                connection,
                returnedConnection));
    }

    [Fact]
    public async Task ChannelFactory_CreatesOpenChannelUsingSharedConnection()
    {
        await using var provider = CreateProvider();
        var channelFactory = new RabbitMqChannelFactory(provider);
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await using var channel = await channelFactory.CreateAsync(
            token: cancellationTokenSource.Token);

        Assert.True(channel.IsOpen);
    }

    [Fact]
    public async Task DisposeAsync_ClosesConnectionAndRejectsFurtherAccess()
    {
        var provider = CreateProvider();
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var connection = await provider.GetConnectionAsync(
            cancellationTokenSource.Token);

        await provider.DisposeAsync();

        Assert.False(connection.IsOpen);
        await Assert.ThrowsAsync<ObjectDisposedException>(
            async () => await provider.GetConnectionAsync());
    }

    [Fact]
    public async Task Dispose_ClosesConnectionAndRejectsFurtherAccess()
    {
        var provider = CreateProvider();
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var connection = await provider.GetConnectionAsync(
            cancellationTokenSource.Token);

        provider.Dispose();
        provider.Dispose();
        await provider.DisposeAsync();

        Assert.False(connection.IsOpen);
        await Assert.ThrowsAsync<ObjectDisposedException>(
            async () => await provider.GetConnectionAsync());
    }

    private RabbitMqConnectionProvider CreateProvider()
    {
        return new RabbitMqConnectionProvider(
            Options.Create(
                new RabbitMqOptions
                {
                    ConnectionString = fixture.ConnectionString
                }));
    }
}
