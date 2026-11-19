using Asyncomnia.Messaging.Reliability.SqlServer.Inbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.Inbox;

public sealed class SqlServerInboxCleanupStoreTests
{
    [Fact]
    public void Constructor_WhenConnectionFactoryIsNull_ThrowsArgumentNullException()
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
        var store = new SqlServerInboxCleanupStore(new TestSqlServerConnectionFactory());

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.DeleteExpiredAsync(DateTimeOffset.UtcNow, batchSize));
    }
}
