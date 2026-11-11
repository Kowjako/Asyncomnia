using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;
using Asyncomnia.Messaging.RabbitMQ.DependencyInjection;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Connection;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;
using Asyncomnia.Messaging.RabbitMQ.Transport;
using Asyncomnia.Messaging.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Moq;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.DependencyInjection;

public sealed class RabbitMqMessagingBuilderExtensionsTests
{
    private const string ConnectionString =
        "amqp://guest:guest@localhost:5672/";

    [Fact]
    public void UseRabbitMq_RegistersOutboundTransportAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMq(options => options.ConnectionString = ConnectionString);

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IOutboundTransport));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(
            typeof(RabbitMqOutboundTransport),
            registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMq_RegistersConnectionProviderAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMq(options =>
            options.ConnectionString = ConnectionString);

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IRabbitMqConnectionProvider));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(
            typeof(RabbitMqConnectionProvider),
            registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMq_RegistersChannelFactoryAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMq(options =>
            options.ConnectionString = ConnectionString);

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IRabbitMqChannelFactory));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(
            typeof(RabbitMqChannelFactory),
            registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMq_RegistersChannelPoolAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMq(options =>
            options.ConnectionString = ConnectionString);

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IRabbitMqChannelPool));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(
            typeof(RabbitMqChannelPool),
            registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMq_RegistersPublisherAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMq(options =>
            options.ConnectionString = ConnectionString);

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IRabbitMqPublisher));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(
            typeof(RabbitMqPublisher),
            registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMq_WhenConnectionProviderIsResolved_AllowsSynchronousServiceProviderDisposal()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();
        builder.UseRabbitMq(options =>
            options.ConnectionString = ConnectionString);
        var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IRabbitMqConnectionProvider>();

        var exception = Record.Exception(provider.Dispose);

        Assert.Null(exception);
    }

    [Fact]
    public void UseRabbitMq_WhenChannelPoolIsResolved_AllowsSynchronousServiceProviderDisposal()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();
        builder.UseRabbitMq(options =>
            options.ConnectionString = ConnectionString);
        var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IRabbitMqChannelPool>();

        var exception = Record.Exception(provider.Dispose);

        Assert.Null(exception);
    }

    [Fact]
    public void UseRabbitMq_RegistersExchangeCreatorAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMq(options =>
            options.ConnectionString = ConnectionString);

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(IRabbitMqExchangeCreator));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(
            typeof(RabbitMqExchangeCreator),
            registration.ImplementationType);
    }

    [Fact]
    public void UseRabbitMq_WhenTransportIsAlreadyRegistered_DoesNotReplaceIt()
    {
        var services = new ServiceCollection();
        var transport = new Mock<IOutboundTransport>().Object;
        services.AddSingleton(transport);
        var builder = services.AddAsyncomniaMessaging();

        builder.UseRabbitMq(options => options.ConnectionString = ConnectionString);

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
        builder.UseRabbitMq(options => options.ConnectionString = ConnectionString);
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<RabbitMqOptions>>().Value;

        Assert.Equal(ConnectionString, options.ConnectionString);
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
    public void UseRabbitMqConsumer_RegistersConsumeErrorStrategyAsSingleton()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncomniaMessaging();

        builder
            .UseRabbitMq(options =>
                options.ConnectionString = ConnectionString)
            .UseRabbitMqConsumer(options =>
                options.EndpointName = "billing-service");

        var registration = Assert.Single(
            services,
            x => x.ServiceType == typeof(RabbitMqConsumeErrorStrategy));
        Assert.Equal(ServiceLifetime.Singleton, registration.Lifetime);
        Assert.Equal(typeof(RabbitMqConsumeErrorStrategy), registration.ImplementationType);
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
