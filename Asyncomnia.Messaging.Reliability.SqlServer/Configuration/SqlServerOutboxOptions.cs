namespace Asyncomnia.Messaging.Reliability.SqlServer.Configuration;

public sealed class SqlServerOutboxOptions
{
    public string ConnectionString { get; set; } = string.Empty;
}