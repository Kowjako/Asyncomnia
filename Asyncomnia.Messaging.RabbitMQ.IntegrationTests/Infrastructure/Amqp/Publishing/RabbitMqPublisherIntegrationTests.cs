using System.Text;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Channels;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Connection;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Asyncomnia.Messaging.RabbitMQ.IntegrationTests.Infrastructure.Amqp.Publishing;

[Collection(RabbitMqCollection.Name)]
public sealed class RabbitMqPublisherIntegrationTests(
    RabbitMqContainerFixture fixture)
{
    [Fact]
    public async Task PublishAsync_WithBoundQueue_PublishesBodyAndProperties()
    {
        var options = CreateOptions();
        await using var connectionProvider =
            new RabbitMqConnectionProvider(options);
        var channelFactory =
            new RabbitMqChannelFactory(connectionProvider);
        await using var channelPool =
            new RabbitMqChannelPool(channelFactory, options);
        var publisher = new RabbitMqPublisher(channelPool);
        await using var topologyChannel =
            await channelFactory.CreateAsync();
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = cancellationTokenSource.Token;
        var exchangeName =
            $"asyncomnia-tests-{Guid.NewGuid():N}";
        const string routingKey = "client-created-event";
        await topologyChannel.ExchangeDeclareAsync(
            exchangeName,
            ExchangeType.Direct,
            durable: false,
            autoDelete: true,
            arguments: null,
            passive: false,
            noWait: false,
            cancellationToken: token);
        var queue = await topologyChannel.QueueDeclareAsync(
            queue: string.Empty,
            durable: false,
            exclusive: true,
            autoDelete: true,
            arguments: null,
            passive: false,
            noWait: false,
            cancellationToken: token);
        await topologyChannel.QueueBindAsync(
            queue.QueueName,
            exchangeName,
            routingKey,
            arguments: null,
            noWait: false,
            cancellationToken: token);
        var messageId = Guid.NewGuid().ToString("D");
        var properties = new BasicProperties
        {
            ContentType = "application/json",
            MessageId = messageId,
            DeliveryMode = DeliveryModes.Persistent
        };
        var body = Encoding.UTF8.GetBytes("{\"name\":\"Acme\"}");

        await publisher.PublishAsync(
            exchangeName,
            routingKey,
            mandatory: true,
            properties,
            body,
            token);

        var result = await topologyChannel.BasicGetAsync(
            queue.QueueName,
            autoAck: true,
            token);

        Assert.NotNull(result);
        Assert.Equal(body, result.Body.ToArray());
        Assert.Equal(messageId, result.BasicProperties.MessageId);
        Assert.Equal(
            "application/json",
            result.BasicProperties.ContentType);
        Assert.Equal(
            DeliveryModes.Persistent,
            result.BasicProperties.DeliveryMode);
    }

    [Fact]
    public async Task PublishAsync_WhenMandatoryMessageCannotBeRouted_ThrowsPublishException()
    {
        var options = CreateOptions();
        await using var connectionProvider =
            new RabbitMqConnectionProvider(options);
        var channelFactory =
            new RabbitMqChannelFactory(connectionProvider);
        await using var channelPool =
            new RabbitMqChannelPool(channelFactory, options);
        var publisher = new RabbitMqPublisher(channelPool);
        await using var topologyChannel =
            await channelFactory.CreateAsync();
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = cancellationTokenSource.Token;
        var exchangeName =
            $"asyncomnia-tests-{Guid.NewGuid():N}";
        await topologyChannel.ExchangeDeclareAsync(
            exchangeName,
            ExchangeType.Direct,
            durable: false,
            autoDelete: true,
            arguments: null,
            passive: false,
            noWait: false,
            cancellationToken: token);

        await Assert.ThrowsAnyAsync<PublishException>(
            () => publisher.PublishAsync(
                exchangeName,
                "missing-binding",
                mandatory: true,
                new BasicProperties(),
                ReadOnlyMemory<byte>.Empty,
                token));
    }

    private IOptions<RabbitMqOptions> CreateOptions()
    {
        return Options.Create(
            new RabbitMqOptions
            {
                ConnectionString = fixture.ConnectionString,
                PublisherChannelCount = 2
            });
    }
}
