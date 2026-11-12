using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Configuration;

internal sealed class SqlServerOptionsValidator : IValidateOptions<SqlServerOptions>
{
    public ValidateOptionsResult Validate(string? name, SqlServerOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return ValidateOptionsResult.Fail("SqlServerOptions:ConnectionString cannot be empty");
        }

        return ValidateOptionsResult.Success;
    }
}
