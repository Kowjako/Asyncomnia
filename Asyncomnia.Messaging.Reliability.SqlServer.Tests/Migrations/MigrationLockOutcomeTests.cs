using Asyncomnia.Messaging.Reliability.SqlServer.Migrations;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Tests.Migrations;

public sealed class MigrationLockOutcomeTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void ClassifyMigrationLockResult_WhenLockIsAcquired_ReturnsAcquired(int lockResult)
    {
        var outcome = SqlServerMigrator.ClassifyMigrationLockResult(lockResult);

        Assert.Equal(MigrationLockOutcome.Acquired, outcome);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(-3)]
    public void ClassifyMigrationLockResult_WhenFailureIsTransient_ReturnsRetry(int lockResult)
    {
        var outcome = SqlServerMigrator.ClassifyMigrationLockResult(lockResult);

        Assert.Equal(MigrationLockOutcome.Retry, outcome);
    }

    [Fact]
    public void ClassifyMigrationLockResult_WhenRequestIsCanceled_ReturnsCanceled()
    {
        var outcome = SqlServerMigrator.ClassifyMigrationLockResult(-2);

        Assert.Equal(MigrationLockOutcome.Canceled, outcome);
    }

    [Theory]
    [InlineData(-4)]
    [InlineData(-999)]
    public void ClassifyMigrationLockResult_WhenFailureIsPermanent_ReturnsFailed(int lockResult)
    {
        var outcome = SqlServerMigrator.ClassifyMigrationLockResult(lockResult);

        Assert.Equal(MigrationLockOutcome.Failed, outcome);
    }
}
