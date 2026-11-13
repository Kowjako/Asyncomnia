using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Inbox;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.Inbox;

public sealed class SqlServerInboxCleanupStoreTests
{
    [Fact]
    public void Constructor_WhenOptionsAreNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new SqlServerInboxCleanupStore(null!));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task DeleteExpiredAsync_WhenBatchSizeIsNotPositive_ThrowsArgumentOutOfRangeException(
        int batchSize)
    {
        var store = new SqlServerInboxCleanupStore(Options.Create(new SqlServerOptions
        {
            ConnectionString = "Server=localhost;Database=AsyncomniaTests"
        }));

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.DeleteExpiredAsync(DateTimeOffset.UtcNow, batchSize));
    }
}
