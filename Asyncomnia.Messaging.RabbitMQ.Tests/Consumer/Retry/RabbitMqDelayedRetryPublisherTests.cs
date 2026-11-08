using System.Text;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using EasyNetQ;
using EasyNetQ.Consumer;
using Microsoft.Extensions.Options;
using Moq;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer.Retry;

public sealed class RabbitMqDelayedRetryPublisherTests
{
    [Fact]
    public async Task TryPublishAsync_WhenBrokerRetriesAreDisabled_ReturnsFalseWithoutPublishing()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var publisher = CreatePublisher(
            advancedBus,
            new RabbitMqConsumerOptions
            {
                BrokerRetriesEnabled = false
            });

        var published = await publisher.TryPublishAsync(CreateContext());

        Assert.False(published);
        advancedBus.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task TryPublishAsync_ForFirstRetry_PublishesOriginalMessageWithIncrementedHeader()
    {
        const string retryHeader = "x-asyncomnia-broker-retry";
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var expiration = TimeSpan.FromMinutes(10);
        var properties = new MessageProperties
        {
            MessageId = Guid.NewGuid().ToString("D"),
            Expiration = expiration,
            Headers = new Dictionary<string, object>
            {
                ["custom-header"] = "custom-value"
            }
        };
        ReadOnlyMemory<byte> body = new byte[] { 1, 2, 3 };
        var context = CreateContext(properties, body);
        var publisher = CreatePublisher(
            advancedBus,
            new RabbitMqConsumerOptions
            {
                BrokerRetriesEnabled = true,
                BrokerRetryDelays = [TimeSpan.FromSeconds(30)]
            });
        advancedBus
            .Setup(x => x.PublishAsync(
                "asyncomnia/retry-30000ms",
                "events/clients/subscription/billing-service",
                true,
                true,
                It.Is<MessageProperties>(publishedProperties =>
                    publishedProperties.MessageId == properties.MessageId &&
                    publishedProperties.Expiration == null &&
                    (int)publishedProperties.Headers[retryHeader] == 1 &&
                    (string)publishedProperties.Headers["custom-header"] == "custom-value"),
                It.Is<ReadOnlyMemory<byte>>(publishedBody => publishedBody.Equals(body)),
                CancellationToken.None))
            .Returns(Task.CompletedTask);

        var published = await publisher.TryPublishAsync(context);

        Assert.True(published);
        Assert.Equal(expiration, context.Properties.Expiration);
        Assert.False(context.Properties.Headers.ContainsKey(retryHeader));
        advancedBus.VerifyAll();
    }

    [Fact]
    public async Task TryPublishAsync_WithRepeatedDelay_UsesTheSameRetryExchangeForTheNextStage()
    {
        const string retryHeader = "x-asyncomnia-broker-retry";
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var context = CreateContext(
            new MessageProperties
            {
                Headers = new Dictionary<string, object>
                {
                    [retryHeader] = 1
                }
            });
        var publisher = CreatePublisher(
            advancedBus,
            new RabbitMqConsumerOptions
            {
                BrokerRetriesEnabled = true,
                BrokerRetryDelays =
                [
                    TimeSpan.FromSeconds(30),
                    TimeSpan.FromSeconds(30)
                ]
            });
        advancedBus
            .Setup(x => x.PublishAsync(
                "asyncomnia/retry-30000ms",
                "events/clients/subscription/billing-service",
                true,
                true,
                It.Is<MessageProperties>(properties =>
                    (int)properties.Headers[retryHeader] == 2),
                It.IsAny<ReadOnlyMemory<byte>>(),
                CancellationToken.None))
            .Returns(Task.CompletedTask);

        var published = await publisher.TryPublishAsync(context);

        Assert.True(published);
        advancedBus.VerifyAll();
    }

    [Fact]
    public async Task TryPublishAsync_WithUtf8RetryHeader_SelectsTheNextRetryStage()
    {
        const string retryHeader = "x-asyncomnia-broker-retry";
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var context = CreateContext(
            new MessageProperties
            {
                Headers = new Dictionary<string, object>
                {
                    [retryHeader] = Encoding.UTF8.GetBytes("1")
                }
            });
        var publisher = CreatePublisher(
            advancedBus,
            new RabbitMqConsumerOptions
            {
                BrokerRetriesEnabled = true,
                BrokerRetryDelays =
                [
                    TimeSpan.FromSeconds(30),
                    TimeSpan.FromMinutes(1)
                ]
            });
        advancedBus
            .Setup(x => x.PublishAsync(
                "asyncomnia/retry-60000ms",
                "events/clients/subscription/billing-service",
                true,
                true,
                It.Is<MessageProperties>(properties =>
                    (int)properties.Headers[retryHeader] == 2),
                It.IsAny<ReadOnlyMemory<byte>>(),
                CancellationToken.None))
            .Returns(Task.CompletedTask);

        var published = await publisher.TryPublishAsync(context);

        Assert.True(published);
        advancedBus.VerifyAll();
    }

    [Fact]
    public async Task TryPublishAsync_WhenAllRetriesAreExhausted_ReturnsFalseWithoutPublishing()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var context = CreateContext(
            new MessageProperties
            {
                Headers = new Dictionary<string, object>
                {
                    ["x-asyncomnia-broker-retry"] = 2
                }
            });
        var publisher = CreatePublisher(
            advancedBus,
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
        advancedBus.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task TryPublishAsync_WhenPublishFails_PropagatesException()
    {
        var advancedBus = new Mock<IAdvancedBus>(MockBehavior.Strict);
        var publishException = new InvalidOperationException("broker unavailable");
        var context = CreateContext();
        var publisher = CreatePublisher(
            advancedBus,
            new RabbitMqConsumerOptions
            {
                BrokerRetriesEnabled = true,
                BrokerRetryDelays = [TimeSpan.FromSeconds(30)]
            });
        advancedBus
            .Setup(x => x.PublishAsync(
                "asyncomnia/retry-30000ms",
                "events/clients/subscription/billing-service",
                true,
                true,
                It.IsAny<MessageProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                CancellationToken.None))
            .ThrowsAsync(publishException);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => publisher.TryPublishAsync(context));

        Assert.Same(publishException, exception);
        advancedBus.VerifyAll();
    }

    private static RabbitMqDelayedRetryPublisher CreatePublisher(Mock<IAdvancedBus> advancedBus, RabbitMqConsumerOptions options)
    {
        return new RabbitMqDelayedRetryPublisher(
            advancedBus.Object,
            Options.Create(options));
    }

    private static ConsumeContext CreateContext(MessageProperties? properties = null, ReadOnlyMemory<byte>? body = null)
    {
        var receivedInfo = new MessageReceivedInfo(
            "consumer",
            1,
            false,
            "events/clients",
            "client-created-event",
            "events/clients/subscription/billing-service");

        return new ConsumeContext(
            receivedInfo,
            properties ?? new MessageProperties(),
            body ?? new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 }),
            Mock.Of<IServiceProvider>(),
            CancellationToken.None);
    }
}
