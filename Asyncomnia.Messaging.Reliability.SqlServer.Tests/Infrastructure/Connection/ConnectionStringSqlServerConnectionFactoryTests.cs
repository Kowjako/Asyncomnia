using Asyncomnia.Messaging.Reliability.SqlServer.Infrastructure.Connection;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.Infrastructure.Connection;

public sealed class ConnectionStringSqlServerConnectionFactoryTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_WhenConnectionStringIsInvalid_ThrowsArgumentException(
        string? connectionString)
    {
        Assert.Throws<ArgumentException>(() =>
            new ConnectionStringSqlServerConnectionFactory(connectionString!));
    }

    [Fact]
    public async Task OpenConnectionAsync_WhenCancellationIsRequested_ThrowsOperationCanceledException()
    {
        var factory = new ConnectionStringSqlServerConnectionFactory(
            "Server=localhost;Database=AsyncomniaTests;Integrated Security=true;TrustServerCertificate=true");
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await factory.OpenConnectionAsync(cancellation.Token));
    }
}
