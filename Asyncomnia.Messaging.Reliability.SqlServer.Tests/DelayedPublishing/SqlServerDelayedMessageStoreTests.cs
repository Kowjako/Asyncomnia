using Asyncomnia.Messaging.Reliability.SqlServer.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.DelayedPublishing;

public sealed class SqlServerDelayedMessageStoreTests
{
    [Fact]
    public void Constructor_WhenConnectionFactoryIsNull_ThrowsArgumentNullException()
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
        var store = new SqlServerDelayedMessageStore(new TestSqlServerConnectionFactory());

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.MoveDueToOutboxAsync(DateTimeOffset.UtcNow, batchSize));
    }
}
