using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.SqlServer.Configuration;

internal sealed class SqlServerOutboxOptionsValidator : IValidateOptions<SqlServerOutboxOptions>
{
    public ValidateOptionsResult Validate(string? name, SqlServerOutboxOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return ValidateOptionsResult.Fail("SqlServerOutboxOptions:ConnectionString cannot be empty");
        }

        return ValidateOptionsResult.Success;
    }
}
