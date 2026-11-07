using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.DependencyInjection;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using Asyncomnia.Messaging.Transport;
using EasyNetQ;
using Microsoft.Extensions.DependencyInjection;
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
            options.EndpointName = "billing-service");
        using var provider = services.BuildServiceProvider();

        var options = provider
            .GetRequiredService<IOptions<RabbitMqConsumerOptions>>()
            .Value;

        Assert.Equal("billing-service", options.EndpointName);
    }
}
