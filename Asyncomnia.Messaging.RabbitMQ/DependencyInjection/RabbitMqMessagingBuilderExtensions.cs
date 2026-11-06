using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using EasyNetQ;
using EasyNetQ.ConnectionString;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.DependencyInjection;

public static class RabbitMqMessagingBuilderExtensions
{
    public static AsyncomniaMessagingBuilder UseRabbitMq(this AsyncomniaMessagingBuilder builder, Action<RabbitMqOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<RabbitMqOptions>, RabbitMqOptionsValidator>());
        builder.Services.AddOptions<RabbitMqOptions>().Configure(configure).ValidateOnStart();

        builder.Services.AddEasyNetQ(serviceProvider =>
        {
            var options = serviceProvider.GetRequiredService<IOptions<RabbitMqOptions>>().Value;
            var parser = serviceProvider.GetRequiredService<IConnectionStringParser>();
            return parser.Parse(options.ConnectionString);
        }).UseSystemTextJson();

        return builder;
    }
}