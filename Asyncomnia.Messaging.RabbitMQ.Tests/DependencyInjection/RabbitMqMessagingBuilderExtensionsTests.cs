using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;
using Asyncomnia.Messaging.RabbitMQ.DependencyInjection;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using Asyncomnia.Messaging.Transport;
using EasyNetQ;
using EasyNetQ.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Moq;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.DependencyInjection;

public sealed class RabbitMqMessagingBuilderExtensionsTests
{
    [Fact]
    public void UseRabbitMq_RegistersOutboundTransportAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMq(options => options.ConnectionString = "host=localhost");

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IOutboundTransport));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal("EasyNetQOutboundTransport", registration.ImplementationType?.Name);
    }

    [Fact]
    public void UseRabbitMq_WhenTransportIsAlreadyRegistered_DoesNotReplaceIt()
    {
        var services = new ServiceCollection();
        var transport = new Mock<IOutboundTransport>().Object;
        services.AddSingleton(transport);
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMq(options => options.ConnectionString = "host=localhost");

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IOutboundTransport));
        Assert.Same(transport, registration.ImplementationInstance);
    }

    [Fact]
    public void UseRabbitMq_ConfiguresRabbitMqOptions()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();
        builder.UseRabbitMq(options => options.ConnectionString = "host=rabbitmq");
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<RabbitMqOptions>>().Value;

        Assert.Equal("host=rabbitmq", options.ConnectionString);
    }

    [Fact]
    public void UseRabbitMq_EnablesPublisherConfirms()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();
        builder.UseRabbitMq(options => options.ConnectionString = "host=localhost");
        using var provider = services.BuildServiceProvider();

        var configuration = provider.GetRequiredService<ConnectionConfiguration>();

        Assert.True(configuration.PublisherConfirms);
    }

    [Fact]
    public void UseRabbitMq_ConfiguresAsyncomniaRoutingConventions()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();
        builder.UseRabbitMq(options => options.ConnectionString = "host=localhost");
        using var provider = services.BuildServiceProvider();
        var conventions = provider.GetRequiredService<IConventions>();

        var exchangeName = conventions.ExchangeNamingConvention(typeof(ClientCreatedEvent));
        var routingKey = conventions.TopicNamingConvention(typeof(ClientCreatedEvent));

        Assert.Equal("events/clients", exchangeName);
        Assert.Equal("client-created-event", routingKey);
    }

    [Fact]
    public void UseRabbitMq_ForNonAsyncomniaMessage_PreservesDefaultConventions()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();
        builder.UseRabbitMq(options => options.ConnectionString = "host=localhost");
        using var provider = services.BuildServiceProvider();
        var conventions = provider.GetRequiredService<IConventions>();
        var defaultConventions = new Conventions(provider.GetRequiredService<ITypeNameSerializer>());

        var exchangeName = conventions.ExchangeNamingConvention(typeof(NonAsyncomniaMessage));
        var routingKey = conventions.TopicNamingConvention(typeof(NonAsyncomniaMessage));

        Assert.Equal(
            defaultConventions.ExchangeNamingConvention(typeof(NonAsyncomniaMessage)),
            exchangeName);
        Assert.Equal(
            defaultConventions.TopicNamingConvention(typeof(NonAsyncomniaMessage)),
            routingKey);
    }

    [Fact]
    public void UseRabbitMqConsumer_RegistersSubscriptionResolverAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMqConsumer(options =>
            options.EndpointName = "billing-service");

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(RabbitMqSubscriptionResolver));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(
            typeof(RabbitMqSubscriptionResolver),
            registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMqConsumer_ConfiguresConsumerOptions()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();
        builder.UseRabbitMqConsumer(options =>
        {
            options.EndpointName = "billing-service";
            options.DeadLetteringEnabled = false;
        });
        using var provider = services.BuildServiceProvider();

        var options = provider
            .GetRequiredService<IOptions<RabbitMqConsumerOptions>>()
            .Value;

        Assert.Equal("billing-service", options.EndpointName);
        Assert.False(options.DeadLetteringEnabled);
    }

    [Fact]
    public void UseRabbitMqConsumer_RegistersDeadLetterPublisherAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMqConsumer(options =>
            options.EndpointName = "billing-service");

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IRabbitMqDeadLetterPublisher));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(
            typeof(RabbitMqDeadLetterPublisher),
            registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMqConsumer_RegistersDeadLetterTopologyCreatorAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMqConsumer(options =>
            options.EndpointName = "billing-service");

        var registration = Assert.Single(
            services,
            x => x.ServiceType ==
                typeof(IRabbitMqDeadLetterTopologyCreator));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(
            typeof(RabbitMqDeadLetterTopologyCreator),
            registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMqConsumer_RegistersRetryTopologyCreatorAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMqConsumer(options =>
            options.EndpointName = "billing-service");

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IRabbitMqRetryTopologyCreator));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(
            typeof(RabbitMqRetryTopologyCreator),
            registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMqConsumer_RegistersDelayedRetryPublisherAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMqConsumer(options =>
            options.EndpointName = "billing-service");

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IRabbitMqDelayedRetryPublisher));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(
            typeof(RabbitMqDelayedRetryPublisher),
            registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMqConsumer_ReplacesEasyNetQConsumeErrorStrategy()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder
            .UseRabbitMq(options =>
                options.ConnectionString = "host=localhost")
            .UseRabbitMqConsumer(options =>
                options.EndpointName = "billing-service");

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IConsumeErrorStrategy));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(
            typeof(RabbitMqConsumeErrorStrategy),
            registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMqConsumer_RegistersTopologyCreatorAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMqConsumer(options =>
            options.EndpointName = "billing-service");

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(RabbitMqTopologyCreator));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(
            typeof(RabbitMqTopologyCreator),
            registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMqConsumer_RegistersMessageConsumerAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMqConsumer(options => options.EndpointName = "billing-service");

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(RabbitMqMessageConsumer));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(typeof(RabbitMqMessageConsumer), registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMqConsumer_RegistersHandlerRegistrarAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMqConsumer(options => options.EndpointName = "billing-service");

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(RabbitMqHandlerRegistrar));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(typeof(RabbitMqHandlerRegistrar), registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMqConsumer_RegistersQueueConsumerFactoryAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMqConsumer(options => options.EndpointName = "billing-service");

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IRabbitMqQueueConsumerFactory));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(typeof(RabbitMqQueueConsumerFactory), registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMqConsumer_RegistersHostedServiceAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMqConsumer(options => options.EndpointName = "billing-service");

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IHostedService)
                 && x.ImplementationType == typeof(RabbitMqConsumerHostedService));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
    }
}
