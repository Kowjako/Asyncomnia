using Asyncomnia.Messaging.Reliability.SqlServer.Configuration;
using Asyncomnia.Messaging.Reliability.SqlServer.Sagas.Retention;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.Sagas;

public sealed class SqlServerSagaCleanupStoreTests
{
    [Fact]
    public void Constructor_WhenOptionsAreNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new SqlServerSagaCleanupStore(null!));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task DeleteFinalizedAsync_WhenRetentionIsNotPositive_ThrowsArgumentOutOfRangeException(
        int retentionSeconds)
    {
        var store = CreateStore();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.DeleteFinalizedAsync(
                TimeSpan.FromSeconds(retentionSeconds),
                batchSize: 10));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task DeleteFinalizedAsync_WhenBatchSizeIsNotPositive_ThrowsArgumentOutOfRangeException(
        int batchSize)
    {
        var store = CreateStore();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.DeleteFinalizedAsync(
                TimeSpan.FromDays(1),
                batchSize));
    }

    private static SqlServerSagaCleanupStore CreateStore()
    {
        return new SqlServerSagaCleanupStore(
            Options.Create(new SqlServerOptions
            {
                ConnectionString = "Server=localhost;Database=AsyncomniaTests"
            }));
    }
}
