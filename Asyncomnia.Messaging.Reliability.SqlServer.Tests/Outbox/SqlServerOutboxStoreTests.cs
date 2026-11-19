using Asyncomnia.Messaging.Reliability.SqlServer.Outbox;
using Asyncomnia.Messaging.Reliability.SqlServer.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.Outbox;

public sealed class SqlServerOutboxStoreTests
{
    [Fact]
    public void Constructor_WhenConnectionFactoryIsNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new SqlServerOutboxStore(null!));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task ClaimAsync_WhenBatchSizeIsNotPositive_ThrowsArgumentOutOfRangeException(
        int batchSize)
    {
        var store = CreateStore();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.ClaimAsync(batchSize, TimeSpan.FromMinutes(1)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task ClaimAsync_WhenLeaseDurationIsNotPositive_ThrowsArgumentOutOfRangeException(
        int leaseSeconds)
    {
        var store = CreateStore();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.ClaimAsync(10, TimeSpan.FromSeconds(leaseSeconds)));
    }

    [Fact]
    public async Task FailAsync_WhenErrorIsNull_ThrowsArgumentNullException()
    {
        var store = CreateStore();

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            store.FailAsync(Guid.NewGuid(), Guid.NewGuid(), null!));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task ScheduleRetryAsync_WhenRetryDelayIsNotPositive_ThrowsArgumentOutOfRangeException(
        int retryDelaySeconds)
    {
        var store = CreateStore();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.ScheduleRetryAsync(
                Guid.NewGuid(),
                Guid.NewGuid(),
                "temporary failure",
                TimeSpan.FromSeconds(retryDelaySeconds)));
    }

    private static SqlServerOutboxStore CreateStore()
    {
        return new SqlServerOutboxStore(new TestSqlServerConnectionFactory());
    }
}
