namespace Asyncomnia.Messaging.Reliability.SqlServer.Migrations;

internal enum MigrationLockOutcome
{
    Acquired,
    Retry,
    Canceled,
    Failed
}
