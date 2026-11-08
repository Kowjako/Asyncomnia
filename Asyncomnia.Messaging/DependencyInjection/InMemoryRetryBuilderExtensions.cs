using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Retry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.DependencyInjection;

public static class InMemoryRetryBuilderExtensions
{
    public static AsyncomniaMessagingBuilder UseInMemoryRetries(this AsyncomniaMessagingBuilder builder, Action<InMemoryRetryOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<InMemoryRetryOptions>, InMemoryRetryOptionsValidator>());

        builder.Services.AddOptions<InMemoryRetryOptions>().Configure(configure).ValidateOnStart();

        return builder;
    }
}