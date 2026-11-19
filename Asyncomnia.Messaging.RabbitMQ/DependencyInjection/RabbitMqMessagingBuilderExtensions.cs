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
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.RabbitMQ.DependencyInjection;

public static class RabbitMqMessagingBuilderExtensions
{
    public static AsyncomniaMessagingBuilder UseRabbitMq(this AsyncomniaMessagingBuilder builder, Action<RabbitMqBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var rabbitMq = new RabbitMqBuilder();

        configure(rabbitMq);

        AddPublisher(builder.Services, rabbitMq);

        if (rabbitMq.ConsumerConfigured)
        {
            AddConsumer(builder.Services, rabbitMq);
        }

        return builder;
    }

    private static void AddPublisher(IServiceCollection services, RabbitMqBuilder configuration)
    {
        services.TryAddSingleton<IRabbitMqConnectionProvider, RabbitMqConnectionProvider>();
        services.TryAddSingleton<IRabbitMqChannelFactory, RabbitMqChannelFactory>();
        services.TryAddSingleton<IRabbitMqChannelPool, RabbitMqChannelPool>();
        services.TryAddSingleton<IRabbitMqPublisher, RabbitMqPublisher>();
        services.TryAddSingleton<IRabbitMqUnroutableTopologyCreator, RabbitMqUnroutableTopologyCreator>();
        services.TryAddSingleton<IRabbitMqExchangeCreator, RabbitMqExchangeCreator>();

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<RabbitMqOptions>, RabbitMqOptionsValidator>());

        services.AddOptions<RabbitMqOptions>().Configure(configuration.ApplyTo).ValidateOnStart();

        services.TryAddSingleton<MessageRouteResolver>();
        services.TryAddSingleton<IOutboundTransport, RabbitMqOutboundTransport>();
    }

    private static void AddConsumer(IServiceCollection services, RabbitMqBuilder configuration)
    {
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<RabbitMqConsumerOptions>, RabbitMqConsumerOptionsValidator>());

        services.AddOptions<RabbitMqConsumerOptions>().Configure(configuration.ApplyConsumerTo).ValidateOnStart();

        services.TryAddSingleton<RabbitMqSubscriptionResolver>();
        services.TryAddSingleton<RabbitMqTopologyCreator>();

        services.TryAddSingleton<RabbitMqDeadLetterRouteResolver>();
        services.TryAddSingleton<IRabbitMqDeadLetterPublisher, RabbitMqDeadLetterPublisher>();
        services.TryAddSingleton<IRabbitMqDeadLetterTopologyCreator, RabbitMqDeadLetterTopologyCreator>();

        services.TryAddSingleton<IRabbitMqRetryTopologyCreator, RabbitMqRetryTopologyCreator>();
        services.TryAddSingleton<IRabbitMqDelayedRetryPublisher, RabbitMqDelayedRetryPublisher>();

        services.TryAddSingleton<RabbitMqConsumeErrorStrategy>();
        services.TryAddSingleton<RabbitMqMessageConsumer>();

        services.TryAddSingleton<IRabbitMqQueueConsumerFactory, RabbitMqQueueConsumerFactory>();
        services.AddHostedService<RabbitMqConsumerHostedService>();
    }
}
