using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Connection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Infrastructure.Amqp.Connection;

public sealed class RabbitMqConnectionProviderTests
{
    [Fact]
    public async Task DisposeAsync_WhenConnectionWasNotCreated_CanBeCalledMoreThanOnce()
    {
        var provider = CreateProvider();

        await provider.DisposeAsync();
        await provider.DisposeAsync();
    }

    [Fact]
    public async Task GetConnectionAsync_AfterProviderWasDisposed_Throws()
    {
        var provider = CreateProvider();
        await provider.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            async () => await provider.GetConnectionAsync());
    }

    [Fact]
    public async Task GetConnectionAsync_WhenCancellationIsAlreadyRequested_Throws()
    {
        var provider = CreateProvider();
        using var cancellationTokenSource =
            new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await provider.GetConnectionAsync(
                cancellationTokenSource.Token));

        await provider.DisposeAsync();
    }

    [Fact]
    public void Constructor_WhenOptionsAreNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => new RabbitMqConnectionProvider(null!));
    }

    private static RabbitMqConnectionProvider CreateProvider()
    {
        return new RabbitMqConnectionProvider(
            Options.Create(
                new RabbitMqOptions
                {
                    ConnectionString =
                        "amqp://guest:guest@localhost:5672/"
                }));
    }
}
