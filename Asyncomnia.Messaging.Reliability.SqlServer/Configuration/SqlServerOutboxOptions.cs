namespace Asyncomnia.Messaging.Reliability.SqlServer.Configuration;

public sealed class SqlServerOutboxOptions
{
    public string ConnectionString { get; set; } = string.Empty;

    public string SchemaName { get; set; } = "asyncomnia";

    public string TableName { get; set; } = "Outbox";
}