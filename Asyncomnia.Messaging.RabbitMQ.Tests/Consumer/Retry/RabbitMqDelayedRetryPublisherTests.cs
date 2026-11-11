using System.Text;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.Retry;

public sealed class RabbitMqDelayedRetryPublisherTests
{
    [Fact]
    public async Task TryPublishAsync_WhenBrokerRetriesAreDisabled_ReturnsFalseWithoutPublishing()
    {
        var brokerPublisher = new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var publisher = CreatePublisher(
            brokerPublisher,
            new RabbitMqConsumerOptions
            {
                BrokerRetriesEnabled = false
            });

        var published = await publisher.TryPublishAsync(CreateContext());

        Assert.False(published);
        brokerPublisher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task TryPublishAsync_ForFirstRetry_PublishesOriginalMessageWithIncrementedHeader()
    {
        const string retryHeader = "x-asyncomnia-broker-retry";
        var brokerPublisher = new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var properties = new BasicProperties
        {
            MessageId = Guid.NewGuid().ToString("D"),
            Expiration = "600000",
            Headers = new Dictionary<string, object?>
            {
                ["custom-header"] = "custom-value"
            }
        };
        ReadOnlyMemory<byte> body = new byte[] { 1, 2, 3 };
        var context = CreateContext(properties, body);
        var publisher = CreatePublisher(
            brokerPublisher,
            new RabbitMqConsumerOptions
            {
                BrokerRetriesEnabled = true,
                BrokerRetryDelays = [TimeSpan.FromSeconds(30)]
            });
        brokerPublisher
            .Setup(x => x.PublishAsync(
                "asyncomnia/retry-30000ms",
                "events/clients/subscription/billing-service",
                true,
                It.Is<BasicProperties>(publishedProperties =>
                    publishedProperties.MessageId == properties.MessageId &&
                    publishedProperties.Expiration == null &&
                    (int)publishedProperties.Headers![retryHeader] == 1 &&
                    (string)publishedProperties.Headers["custom-header"] == "custom-value"),
                It.Is<ReadOnlyMemory<byte>>(publishedBody => publishedBody.Equals(body)),
                CancellationToken.None))
            .Returns(Task.CompletedTask);

        var published = await publisher.TryPublishAsync(context);

        Assert.True(published);
        Assert.Equal("600000", context.Properties.Expiration);
        Assert.False(context.Properties.Headers!.ContainsKey(retryHeader));
        brokerPublisher.VerifyAll();
    }

    [Fact]
    public async Task TryPublishAsync_WithRepeatedDelay_UsesTheSameRetryExchangeForTheNextStage()
    {
        const string retryHeader = "x-asyncomnia-broker-retry";
        var brokerPublisher = new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var context = CreateContext(
            new BasicProperties
            {
                Headers = new Dictionary<string, object?>
                {
                    [retryHeader] = 1
                }
            });
        var publisher = CreatePublisher(
            brokerPublisher,
            new RabbitMqConsumerOptions
            {
                BrokerRetriesEnabled = true,
                BrokerRetryDelays =
                [
                    TimeSpan.FromSeconds(30),
                    TimeSpan.FromSeconds(30)
                ]
            });
        brokerPublisher
            .Setup(x => x.PublishAsync(
                "asyncomnia/retry-30000ms",
                "events/clients/subscription/billing-service",
                true,
                It.Is<BasicProperties>(properties =>(int)properties.Headers![retryHeader] == 2),
                It.IsAny<ReadOnlyMemory<byte>>(),
                CancellationToken.None))
            .Returns(Task.CompletedTask);

        var published = await publisher.TryPublishAsync(context);

        Assert.True(published);
        brokerPublisher.VerifyAll();
    }

    [Fact]
    public async Task TryPublishAsync_WithUtf8RetryHeader_SelectsTheNextRetryStage()
    {
        const string retryHeader = "x-asyncomnia-broker-retry";
        var brokerPublisher = new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var context = CreateContext(
            new BasicProperties
            {
                Headers = new Dictionary<string, object?>
                {
                    [retryHeader] = Encoding.UTF8.GetBytes("1")
                }
            });
        var publisher = CreatePublisher(
            brokerPublisher,
            new RabbitMqConsumerOptions
            {
                BrokerRetriesEnabled = true,
                BrokerRetryDelays =
                [
                    TimeSpan.FromSeconds(30),
                    TimeSpan.FromMinutes(1)
                ]
            });
        brokerPublisher
            .Setup(x => x.PublishAsync(
                "asyncomnia/retry-60000ms",
                "events/clients/subscription/billing-service",
                true,
                It.Is<BasicProperties>(properties =>
                    (int)properties.Headers![retryHeader] == 2),
                It.IsAny<ReadOnlyMemory<byte>>(),
                CancellationToken.None))
            .Returns(Task.CompletedTask);

        var published = await publisher.TryPublishAsync(context);

        Assert.True(published);
        brokerPublisher.VerifyAll();
    }

    [Fact]
    public async Task TryPublishAsync_WhenAllRetriesAreExhausted_ReturnsFalseWithoutPublishing()
    {
        var brokerPublisher = new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var context = CreateContext(
            new BasicProperties
            {
                Headers = new Dictionary<string, object?>
                {
                    ["x-asyncomnia-broker-retry"] = 2
                }
            });
        var publisher = CreatePublisher(
            brokerPublisher,
            new RabbitMqConsumerOptions
            {
                BrokerRetriesEnabled = true,
                BrokerRetryDelays =
                [
                    TimeSpan.FromSeconds(30),
                    TimeSpan.FromMinutes(1)
                ]
            });

        var published = await publisher.TryPublishAsync(context);

        Assert.False(published);
        brokerPublisher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task TryPublishAsync_WhenPublishFails_PropagatesException()
    {
        var brokerPublisher = new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var publishException = new InvalidOperationException("broker unavailable");
        var context = CreateContext();
        var publisher = CreatePublisher(
            brokerPublisher,
            new RabbitMqConsumerOptions
            {
                BrokerRetriesEnabled = true,
                BrokerRetryDelays = [TimeSpan.FromSeconds(30)]
            });
        brokerPublisher
            .Setup(x => x.PublishAsync(
                "asyncomnia/retry-30000ms",
                "events/clients/subscription/billing-service",
                true,
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                CancellationToken.None))
            .ThrowsAsync(publishException);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => publisher.TryPublishAsync(context));

        Assert.Same(publishException, exception);
        brokerPublisher.VerifyAll();
    }

    [Fact]
    public async Task TryPublishAsync_WhenContextIsNull_Throws()
    {
        var brokerPublisher = new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var publisher = CreatePublisher(
            brokerPublisher,
            new RabbitMqConsumerOptions());

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => publisher.TryPublishAsync(null!));

        brokerPublisher.VerifyNoOtherCalls();
    }

    private static RabbitMqDelayedRetryPublisher CreatePublisher(
        Mock<IRabbitMqPublisher> brokerPublisher,
        RabbitMqConsumerOptions options)
    {
        return new RabbitMqDelayedRetryPublisher(
            brokerPublisher.Object,
            Options.Create(options));
    }

    private static RabbitMqConsumeContext CreateContext(
        IReadOnlyBasicProperties? properties = null,
        ReadOnlyMemory<byte>? body = null)
    {
        return new RabbitMqConsumeContext(
            "events/clients/subscription/billing-service",
            "events/clients",
            "client-created-event",
            properties ?? new BasicProperties(),
            body ?? new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 }));
    }
}
