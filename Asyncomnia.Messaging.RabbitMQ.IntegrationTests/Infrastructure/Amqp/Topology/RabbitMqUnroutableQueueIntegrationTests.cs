using System.Text;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology.Arguments;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.IntegrationTests.Infrastructure.Amqp.Topology;

[Collection(RabbitMqCollection.Name)]
public sealed class RabbitMqUnroutableQueueIntegrationTests(
    RabbitMqContainerFixture fixture)
{
    [Fact]
    public async Task QueueArguments_WhenLimitIsReached_DropOldestMessage()
    {
        using var cancellation =
            new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = cancellation.Token;
        var connectionFactory = new ConnectionFactory
        {
            Uri = new Uri(fixture.ConnectionString)
        };
        await using var connection =
            await connectionFactory.CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(
            cancellationToken: token);
        var exchangeName = $"asyncomnia-tests-{Guid.NewGuid():N}";
        var queueName = $"asyncomnia-tests-{Guid.NewGuid():N}";
        await channel.ExchangeDeclareAsync(
            exchangeName,
            ExchangeType.Fanout,
            durable: false,
            autoDelete: true,
            arguments: null,
            passive: false,
            noWait: false,
            cancellationToken: token);
        await channel.QueueDeclareAsync(
            queueName,
            durable: false,
            exclusive: true,
            autoDelete: true,
            arguments: RabbitMqQueueArgumentsFactory.Create(
                new RabbitMqQueueDeclarationSettings(
                    QueueType: RabbitMqQueueType.Classic,
                    MaxLength: 2,
                    MaxLengthBytes: 1024 * 1024,
                    Overflow: RabbitMqQueueOverflowStrategy.DropHead)),
            passive: false,
            noWait: false,
            cancellationToken: token);
        await channel.QueueBindAsync(
            queueName,
            exchangeName,
            routingKey: string.Empty,
            arguments: null,
            noWait: false,
            cancellationToken: token);

        await PublishAsync(channel, exchangeName, "first", token);
        await PublishAsync(channel, exchangeName, "second", token);
        await PublishAsync(channel, exchangeName, "third", token);

        var firstRemaining = await channel.BasicGetAsync(
            queueName,
            autoAck: true,
            cancellationToken: token);
        var secondRemaining = await channel.BasicGetAsync(
            queueName,
            autoAck: true,
            cancellationToken: token);
        var noMoreMessages = await channel.BasicGetAsync(
            queueName,
            autoAck: true,
            cancellationToken: token);

        Assert.NotNull(firstRemaining);
        Assert.NotNull(secondRemaining);
        Assert.Equal(
            "second",
            Encoding.UTF8.GetString(firstRemaining.Body.Span));
        Assert.Equal(
            "third",
            Encoding.UTF8.GetString(secondRemaining.Body.Span));
        Assert.Null(noMoreMessages);
    }

    private static ValueTask PublishAsync(
        IChannel channel,
        string exchangeName,
        string value,
        CancellationToken token)
    {
        return channel.BasicPublishAsync(
            exchange: exchangeName,
            routingKey: string.Empty,
            mandatory: false,
            basicProperties: new BasicProperties(),
            body: Encoding.UTF8.GetBytes(value),
            cancellationToken: token);
    }
}
