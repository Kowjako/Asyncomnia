namespace Asyncomnia.Messaging.Reliability.SqlServer.Configuration;

internal static class SqlServerOutboxDefaults
{
    public const string SchemaName = "asyncomnia";

    public const string TableName = "Outbox";

    public const string QualifiedTableName = "[asyncomnia].[Outbox]";
}