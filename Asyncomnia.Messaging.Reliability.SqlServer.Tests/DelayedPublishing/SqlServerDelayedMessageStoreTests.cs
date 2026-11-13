using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.DelayedPublishing;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.DelayedPublishing;

public sealed class SqlServerDelayedMessageStoreTests
{
    [Fact]
    public void Constructor_WhenOptionsAreNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new SqlServerDelayedMessageStore(null!));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task MoveDueToOutboxAsync_WhenBatchSizeIsNotPositive_ThrowsArgumentOutOfRangeException(
        int batchSize)
    {
        var store = new SqlServerDelayedMessageStore(
            Options.Create(new SqlServerOptions
            {
                ConnectionString = "Server=localhost;Database=AsyncomniaTests"
            }));

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.MoveDueToOutboxAsync(DateTimeOffset.UtcNow, batchSize));
    }
}
