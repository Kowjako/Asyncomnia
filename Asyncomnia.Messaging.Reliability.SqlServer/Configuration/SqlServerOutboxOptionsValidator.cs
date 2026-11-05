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

        if (string.IsNullOrWhiteSpace(options.SchemaName))
        {
            return ValidateOptionsResult.Fail("SqlServerOutboxOptions:SchemaName cannot be empty");
        }

        if (!IsValidIdentifier(options.SchemaName))
        {
            return ValidateOptionsResult.Fail("SqlServerOutboxOptions:SchemaName is not a valid SQL identifier");
        }

        if (string.IsNullOrWhiteSpace(options.TableName))
        {
            return ValidateOptionsResult.Fail("SqlServerOutboxOptions:TableName cannot be empty");
        }

        if (!IsValidIdentifier(options.TableName))
        {
            return ValidateOptionsResult.Fail("SqlServerOutboxOptions:TableName is not a valid SQL identifier");
        }

        return ValidateOptionsResult.Success;
    }

    private static bool IsValidIdentifier(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        if (!char.IsLetter(value[0]) && value[0] != '_')
        {
            return false;
        }

        return value.All(character => char.IsLetterOrDigit(character) || character == '_');
    }
}
