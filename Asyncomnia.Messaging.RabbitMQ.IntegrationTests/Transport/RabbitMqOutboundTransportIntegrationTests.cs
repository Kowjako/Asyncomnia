using System.Text.Json;
using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.RabbitMQ.DependencyInjection;
using Asyncomnia.Messaging.RabbitMQ.IntegrationTests.Infrastructure;
using Asyncomnia.Messaging.RabbitMQ.IntegrationTests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Asyncomnia.Messaging.RabbitMQ.IntegrationTests.Transport;

[Collection(RabbitMqCollection.Name)]
public sealed class RabbitMqOutboundTransportIntegrationTests
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly string _connectionString;

    public RabbitMqOutboundTransportIntegrationTests(RabbitMqContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task PublishAsync_RoutesEventAndPreservesBrokerMetadata()
    {
        await using var provider = CreateProvider();
        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = cancellationTokenSource.Token;
        await using var connection = await CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        var queueName = await DeclareBoundQueueAsync(
            channel,
            "events/clients",
            "client-created-event",
            token);
        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();
        var @event = new ClientCreatedEvent(Guid.NewGuid(), "Acme");
        var correlationId = Guid.NewGuid();
        var earliestTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        await publisher.PublishAsync(
            @event,
            new MessageDispatchOptions { CorrelationId = correlationId },
            token);

        var result = await PullAvailableAsync(channel, queueName, token);
        var latestTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var receivedEvent = JsonSerializer.Deserialize<ClientCreatedEvent>(
            result.Body.Span,
            JsonOptions);

        Assert.Equal(@event, receivedEvent);
        Assert.Equal("events/clients", result.Exchange);
        Assert.Equal("client-created-event", result.RoutingKey);
        Assert.True(Guid.TryParse(result.BasicProperties.MessageId, out var messageId));
        Assert.NotEqual(Guid.Empty, messageId);
        Assert.Equal(correlationId.ToString("D"), result.BasicProperties.CorrelationId);
        Assert.InRange(
            result.BasicProperties.Timestamp.UnixTime,
            earliestTimestamp,
            latestTimestamp);
        Assert.Equal(DeliveryModes.Persistent, result.BasicProperties.DeliveryMode);
        Assert.Contains(nameof(ClientCreatedEvent), result.BasicProperties.Type);
    }

    [Fact]
    public async Task PublishAsync_WithExplicitRoutingKey_RoutesEventToConfiguredBinding()
    {
        await using var provider = CreateProvider();
        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = cancellationTokenSource.Token;
        await using var connection = await CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        var queueName = await DeclareBoundQueueAsync(
            channel,
            "events/clients",
            "client-contract-signed",
            token);
        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();
        var @event = new ClientContractSignedEvent(Guid.NewGuid(), "C-2026-001");

        await publisher.PublishAsync(@event, token: token);

        var result = await PullAvailableAsync(channel, queueName, token);
        var receivedEvent = JsonSerializer.Deserialize<ClientContractSignedEvent>(
            result.Body.Span,
            JsonOptions);

        Assert.Equal(@event, receivedEvent);
        Assert.Equal("events/clients", result.Exchange);
        Assert.Equal("client-contract-signed", result.RoutingKey);
    }

    [Fact]
    public async Task SendAsync_WhenCommandCannotBeRouted_ThrowsPublishException()
    {
        await using var provider = CreateProvider();
        await using var scope = provider.CreateAsyncScope();
        var sender = scope.ServiceProvider.GetRequiredService<ICommandSender>();
        var command = new StartUnhandledJobCommand(Guid.NewGuid());
        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await Assert.ThrowsAnyAsync<PublishException>(
            () => sender.SendAsync(command, token: cancellationTokenSource.Token));
    }

    private ServiceProvider CreateProvider()
    {
        var services = new ServiceCollection();

        services
            .AddAsyncomniaMessaging()
            .UseRabbitMq(options => options.ConnectionString = _connectionString);

        return services.BuildServiceProvider();
    }

    private Task<IConnection> CreateConnectionAsync(CancellationToken token)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_connectionString)
        };

        return factory.CreateConnectionAsync(token);
    }

    private static async Task<string> DeclareBoundQueueAsync(
        IChannel channel,
        string exchangeName,
        string routingKey,
        CancellationToken token)
    {
        await channel.ExchangeDeclareAsync(
            exchangeName,
            ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            arguments: null,
            passive: false,
            noWait: false,
            cancellationToken: token);

        var queue = await channel.QueueDeclareAsync(
            $"asyncomnia-tests-{Guid.NewGuid():N}",
            durable: false,
            exclusive: false,
            autoDelete: true,
            arguments: null,
            passive: false,
            noWait: false,
            cancellationToken: token);

        await channel.QueueBindAsync(
            queue.QueueName,
            exchangeName,
            routingKey,
            arguments: null,
            noWait: false,
            cancellationToken: token);

        return queue.QueueName;
    }

    private static async Task<BasicGetResult> PullAvailableAsync(
        IChannel channel,
        string queueName,
        CancellationToken token)
    {
        while (true)
        {
            var result = await channel.BasicGetAsync(
                queueName,
                autoAck: true,
                cancellationToken: token);

            if (result is not null)
            {
                return result;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), token);
        }
    }
}
