using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Connection;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;
using Asyncomnia.Messaging.RabbitMQ.Routing;
using Asyncomnia.Messaging.RabbitMQ.Transport;
using Asyncomnia.Messaging.Transport;
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

        builder.Services.TryAddSingleton<IRabbitMqConnectionProvider, RabbitMqConnectionProvider>();
        builder.Services.TryAddSingleton<IRabbitMqChannelFactory, RabbitMqChannelFactory>();
        builder.Services.TryAddSingleton<IRabbitMqChannelPool, RabbitMqChannelPool>();
        builder.Services.TryAddSingleton<IRabbitMqPublisher, RabbitMqPublisher>();

        builder.Services.TryAddSingleton<IRabbitMqExchangeCreator, RabbitMqExchangeCreator>();

        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<RabbitMqOptions>, RabbitMqOptionsValidator>());
        builder.Services.AddOptions<RabbitMqOptions>().Configure(configure).ValidateOnStart();

        builder.Services.AddEasyNetQ(serviceProvider =>
        {
            var options = serviceProvider.GetRequiredService<IOptions<RabbitMqOptions>>().Value;
            var parser = serviceProvider.GetRequiredService<IConnectionStringParser>();
            var configuration = parser.Parse(options.ConnectionString);
            configuration.PublisherConfirms = true; // enforce publisher confirms

            return configuration;
        });

        builder.Services.TryAddSingleton<MessageRouteResolver>();

        builder.Services.RegisterAsyncomniaRoutingConventions();

        builder.Services.TryAddSingleton<IOutboundTransport, RabbitMqOutboundTransport>();

        return builder;
    }

    public static AsyncomniaMessagingBuilder UseRabbitMqConsumer(this AsyncomniaMessagingBuilder builder, Action<RabbitMqConsumerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<RabbitMqConsumerOptions>, RabbitMqConsumerOptionsValidator>());

        builder.Services.AddOptions<RabbitMqConsumerOptions>().Configure(configure).ValidateOnStart();

        builder.Services.TryAddSingleton<RabbitMqSubscriptionResolver>();
        builder.Services.TryAddSingleton<RabbitMqTopologyCreator>();

        builder.Services.TryAddSingleton<RabbitMqDeadLetterRouteResolver>();
        builder.Services.TryAddSingleton<IRabbitMqDeadLetterPublisher, RabbitMqDeadLetterPublisher>();
        builder.Services.TryAddSingleton<IRabbitMqDeadLetterTopologyCreator, RabbitMqDeadLetterTopologyCreator>();

        builder.Services.TryAddSingleton<IRabbitMqRetryTopologyCreator, RabbitMqRetryTopologyCreator>();
        builder.Services.TryAddSingleton<IRabbitMqDelayedRetryPublisher, RabbitMqDelayedRetryPublisher>();

        builder.Services.TryAddSingleton<RabbitMqMessageConsumer>();
        builder.Services.TryAddSingleton<RabbitMqHandlerRegistrar>();

        builder.Services.TryAddSingleton<IRabbitMqQueueConsumerFactory, RabbitMqQueueConsumerFactory>();

        builder.Services.AddHostedService<RabbitMqConsumerHostedService>();

        return builder;
    }

    private static void RegisterAsyncomniaRoutingConventions(this IServiceCollection services)
    {
        services.Replace(ServiceDescriptor.Singleton<IConventions>(serviceProvider =>
        {
            var typeNameSerializer = serviceProvider.GetRequiredService<ITypeNameSerializer>();
            var routeResolver = serviceProvider.GetRequiredService<MessageRouteResolver>();

            var conventions = new Conventions(typeNameSerializer);

            var defaultExchangeNaming = conventions.ExchangeNamingConvention;
            var defaultTopicNaming = conventions.TopicNamingConvention;

            conventions.ExchangeNamingConvention = messageType =>
                typeof(IMessage).IsAssignableFrom(messageType)
                    ? routeResolver.Resolve(messageType).ExchangeName
                    : defaultExchangeNaming(messageType);

            conventions.TopicNamingConvention = messageType =>
                typeof(IMessage).IsAssignableFrom(messageType)
                    ? routeResolver.Resolve(messageType).RoutingKey
                    : defaultTopicNaming(messageType);

            return conventions;
        }));
    }
}