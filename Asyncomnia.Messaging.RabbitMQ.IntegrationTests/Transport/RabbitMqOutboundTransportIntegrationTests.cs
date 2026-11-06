using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.RabbitMQ.DependencyInjection;
using Asyncomnia.Messaging.RabbitMQ.IntegrationTests.Infrastructure;
using Asyncomnia.Messaging.RabbitMQ.IntegrationTests.TestDoubles;
using EasyNetQ;
using EasyNetQ.Producer;
using EasyNetQ.Topology;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.RabbitMQ.IntegrationTests.Transport;

[Collection(RabbitMqCollection.Name)]
public sealed class RabbitMqOutboundTransportIntegrationTests
{
    private readonly string _connectionString;

    public RabbitMqOutboundTransportIntegrationTests(RabbitMqContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task PublishAsync_RoutesEventAndPreservesBrokerMetadata()
    {
        await using var provider = CreateProvider();
        var advancedBus = provider.GetRequiredService<IAdvancedBus>();

        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var cancellationToken = cancellationTokenSource.Token;
        var queue = await DeclareBoundQueueAsync(advancedBus, "events/clients", "client-created-event", cancellationToken);
        var consumer = advancedBus.CreatePullingConsumer<ClientCreatedEvent>(queue);

        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();

        var @event = new ClientCreatedEvent(Guid.NewGuid(), "Acme");
        var correlationId = Guid.NewGuid();
        var earliestTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        await publisher.PublishAsync(
            @event,
            new MessageDispatchOptions { CorrelationId = correlationId },
            cancellationToken);

        var result = await PullAvailableAsync(consumer, cancellationToken);
        var latestTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        Assert.Equal(@event, result.Message.Body);
        Assert.Equal("events/clients", result.ReceivedInfo.Exchange);
        Assert.Equal("client-created-event", result.ReceivedInfo.RoutingKey);
        Assert.True(Guid.TryParse(result.Message.Properties.MessageId, out var messageId));
        Assert.NotEqual(Guid.Empty, messageId);
        Assert.Equal(correlationId.ToString("D"), result.Message.Properties.CorrelationId);
        Assert.InRange(result.Message.Properties.Timestamp, earliestTimestamp, latestTimestamp);
        Assert.Equal((byte)2, result.Message.Properties.DeliveryMode);
        Assert.Contains(nameof(ClientCreatedEvent), result.Message.Properties.Type);
    }

    [Fact]
    public async Task PublishAsync_WithExplicitRoutingKey_RoutesEventToConfiguredBinding()
    {
        await using var provider = CreateProvider();
        var advancedBus = provider.GetRequiredService<IAdvancedBus>();

        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var cancellationToken = cancellationTokenSource.Token;

        var queue = await DeclareBoundQueueAsync(advancedBus, "events/clients", "client-contract-signed", cancellationToken);

        var consumer = advancedBus.CreatePullingConsumer<ClientContractSignedEvent>(queue);
        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();
        var @event = new ClientContractSignedEvent(Guid.NewGuid(), "C-2026-001");

        await publisher.PublishAsync(@event, token: cancellationToken);
        var result = await PullAvailableAsync(consumer, cancellationToken);

        Assert.Equal(@event, result.Message.Body);
        Assert.Equal("events/clients", result.ReceivedInfo.Exchange);
        Assert.Equal("client-contract-signed", result.ReceivedInfo.RoutingKey);
    }

    [Fact]
    public async Task SendAsync_WhenCommandCannotBeRouted_ThrowsPublishReturnedException()
    {
        await using var provider = CreateProvider();
        await using var scope = provider.CreateAsyncScope();
        var sender = scope.ServiceProvider.GetRequiredService<ICommandSender>();
        var command = new StartUnhandledJobCommand(Guid.NewGuid());
        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await Assert.ThrowsAsync<PublishReturnedException>(() => sender.SendAsync(command, token: cancellationTokenSource.Token));
    }

    private ServiceProvider CreateProvider()
    {
        var services = new ServiceCollection();

        services
            .AddAsyncomniaMessaging()
            .UseRabbitMq(options => options.ConnectionString = _connectionString);

        return services.BuildServiceProvider();
    }

    private static async Task<Queue> DeclareBoundQueueAsync(IAdvancedBus advancedBus, string exchangeName, string routingKey, CancellationToken cancellationToken)
    {
        var exchange = await advancedBus.ExchangeDeclareAsync(
            exchangeName,
            ExchangeType.Topic,
            cancellationToken: cancellationToken);

        var queue = await advancedBus.QueueDeclareAsync(
            $"asyncomnia-tests-{Guid.NewGuid():N}",
            configuration => configuration.AsDurable(false).AsAutoDelete(true),
            cancellationToken);

        await advancedBus.QueueBindAsync(queue.Name, exchange.Name, routingKey, cancellationToken: cancellationToken);

        return queue;
    }

    private static async Task<PullResult<T>> PullAvailableAsync<T>(IPullingConsumer<PullResult<T>> consumer, CancellationToken cancellationToken)
    {
        while (true)
        {
            var result = await consumer.PullAsync(cancellationToken);

            if (result.IsAvailable)
            {
                return result;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
        }
    }
}
