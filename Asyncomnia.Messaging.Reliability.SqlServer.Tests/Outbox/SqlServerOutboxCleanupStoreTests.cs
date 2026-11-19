using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Outbox.Retention;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.Outbox;

public sealed class SqlServerOutboxCleanupStoreTests
{
    [Fact]
    public void Constructor_WhenOptionsAreNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new SqlServerOutboxCleanupStore(null!));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task DeleteExpiredAsync_WhenProcessedRetentionIsNotPositive_ThrowsArgumentOutOfRangeException(
        int retentionSeconds)
    {
        var store = CreateStore();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.DeleteExpiredAsync(
                TimeSpan.FromSeconds(retentionSeconds),
                failedMessageRetention: null,
                batchSize: 10));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task DeleteExpiredAsync_WhenFailedRetentionIsNotPositive_ThrowsArgumentOutOfRangeException(
        int retentionSeconds)
    {
        var store = CreateStore();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.DeleteExpiredAsync(
                TimeSpan.FromDays(1),
                TimeSpan.FromSeconds(retentionSeconds),
                batchSize: 10));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task DeleteExpiredAsync_WhenBatchSizeIsNotPositive_ThrowsArgumentOutOfRangeException(
        int batchSize)
    {
        var store = CreateStore();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.DeleteExpiredAsync(
                TimeSpan.FromDays(1),
                failedMessageRetention: null,
                batchSize: batchSize));
    }

    private static SqlServerOutboxCleanupStore CreateStore()
    {
        return new SqlServerOutboxCleanupStore(
            Options.Create(new SqlServerOptions
            {
                ConnectionString = "Server=localhost;Database=AsyncomniaTests"
            }));
    }
}
