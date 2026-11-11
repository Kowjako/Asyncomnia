using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Internal.Serialization;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Serialization;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer;

public sealed class RabbitMqMessageConsumerTests
{
    [Fact]
    public async Task ConsumeAsync_WhenPipelineSucceeds_MapsEnvelopeAndAcknowledgesMessage()
    {
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(2026, 11, 7, 15, 30, 0, TimeSpan.Zero);
        var body = new ClientCreatedEvent("Acme");
        MessageEnvelope<ClientCreatedEvent>? capturedEnvelope = null;
        var pipeline = new Mock<IInboundMessagePipeline>(MockBehavior.Strict);
        pipeline
            .Setup(x => x.HandleEventAsync(
                It.IsAny<MessageEnvelope<ClientCreatedEvent>>(),
                CancellationToken.None))
            .Callback<MessageEnvelope<ClientCreatedEvent>, CancellationToken>(
                (envelope, _) => capturedEnvelope = envelope)
            .Returns(Task.CompletedTask);
        await using var provider = CreateProvider(_ => pipeline.Object);
        var errorDependencies = CreateErrorStrategy(deadLetteringEnabled: false);
        var consumer = CreateConsumer(provider, errorDependencies.Strategy);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        channel
            .Setup(x => x.BasicAckAsync(1, false, CancellationToken.None))
            .Returns(ValueTask.CompletedTask);
        var delivery = CreateDelivery(
            body,
            new MessageMetadata(messageId, createdAt)
            {
                CorrelationId = correlationId
            });

        await consumer.ConsumeAsync(
            channel.Object,
            CreateSubscription(),
            delivery,
            CancellationToken.None);

        Assert.NotNull(capturedEnvelope);
        Assert.Equal(body, capturedEnvelope.Message);
        Assert.Equal(messageId, capturedEnvelope.Metadata.MessageId);
        Assert.Equal(correlationId, capturedEnvelope.Metadata.CorrelationId);
        Assert.Equal(createdAt, capturedEnvelope.Metadata.CreatedAt);
        pipeline.VerifyAll();
        channel.VerifyAll();
        errorDependencies.VerifyNoCalls();
    }

    [Fact]
    public async Task ConsumeAsync_WhenPipelineThrows_DisposesScopeAndNacksWithoutRequeue()
    {
        var expectedException = new InvalidOperationException("handler failed");
        var instances = new List<DisposableInboundMessagePipeline>();
        await using var provider = CreateProvider(_ =>
        {
            var pipeline = new DisposableInboundMessagePipeline(expectedException);
            instances.Add(pipeline);
            return pipeline;
        });
        var errorDependencies = CreateErrorStrategy(deadLetteringEnabled: false);
        errorDependencies.DelayedRetryPublisher
            .Setup(x => x.TryPublishAsync(
                It.IsAny<RabbitMqConsumeContext>(),
                CancellationToken.None))
            .ReturnsAsync(false);
        var consumer = CreateConsumer(provider, errorDependencies.Strategy);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        channel
            .Setup(x => x.BasicNackAsync(1, false, false, CancellationToken.None))
            .Returns(ValueTask.CompletedTask);

        await consumer.ConsumeAsync(
            channel.Object,
            CreateSubscription(),
            CreateDelivery(new ClientCreatedEvent("Acme")),
            CancellationToken.None);

        Assert.True(Assert.Single(instances).IsDisposed);
        errorDependencies.DelayedRetryPublisher.VerifyAll();
        errorDependencies.DeadLetterPublisher.VerifyNoOtherCalls();
        channel.VerifyAll();
    }

    [Fact]
    public async Task ConsumeAsync_ForEachMessage_CreatesAndDisposesSeparateScope()
    {
        var instances = new List<DisposableInboundMessagePipeline>();
        await using var provider = CreateProvider(_ =>
        {
            var pipeline = new DisposableInboundMessagePipeline();
            instances.Add(pipeline);
            return pipeline;
        });
        var errorDependencies = CreateErrorStrategy(deadLetteringEnabled: false);
        var consumer = CreateConsumer(provider, errorDependencies.Strategy);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        channel
            .Setup(x => x.BasicAckAsync(It.IsAny<ulong>(), false, CancellationToken.None))
            .Returns(ValueTask.CompletedTask);

        await consumer.ConsumeAsync(
            channel.Object,
            CreateSubscription(),
            CreateDelivery(new ClientCreatedEvent("Acme"), deliveryTag: 1),
            CancellationToken.None);
        await consumer.ConsumeAsync(
            channel.Object,
            CreateSubscription(),
            CreateDelivery(new ClientCreatedEvent("Contoso"), deliveryTag: 2),
            CancellationToken.None);

        Assert.Equal(2, instances.Count);
        Assert.NotSame(instances[0], instances[1]);
        Assert.All(instances, pipeline => Assert.True(pipeline.IsDisposed));
        channel.Verify(
            x => x.BasicAckAsync(It.IsAny<ulong>(), false, CancellationToken.None),
            Times.Exactly(2));
        errorDependencies.VerifyNoCalls();
    }

    [Fact]
    public async Task ConsumeAsync_WithInvalidMetadata_DoesNotCreateScopeAndNacksWithoutRequeue()
    {
        var createdPipelines = 0;
        await using var provider = CreateProvider(_ =>
        {
            createdPipelines++;
            return new DisposableInboundMessagePipeline();
        });
        var errorDependencies = CreateErrorStrategy(deadLetteringEnabled: false);
        errorDependencies.DelayedRetryPublisher
            .Setup(x => x.TryPublishAsync(
                It.IsAny<RabbitMqConsumeContext>(),
                CancellationToken.None))
            .ReturnsAsync(false);
        var consumer = CreateConsumer(provider, errorDependencies.Strategy);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        channel
            .Setup(x => x.BasicNackAsync(1, false, false, CancellationToken.None))
            .Returns(ValueTask.CompletedTask);
        var properties = RabbitMqMessagePropertiesMapper.MapToBrokerProperties(
            typeof(ClientCreatedEvent),
            new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow));
        properties.MessageId = "invalid";
        var delivery = CreateDelivery(
            new ClientCreatedEvent("Acme"),
            properties);

        await consumer.ConsumeAsync(
            channel.Object,
            CreateSubscription(),
            delivery,
            CancellationToken.None);

        Assert.Equal(0, createdPipelines);
        errorDependencies.DelayedRetryPublisher.VerifyAll();
        errorDependencies.DeadLetterPublisher.VerifyNoOtherCalls();
        channel.VerifyAll();
    }

    [Fact]
    public async Task ConsumeAsync_WhenProcessingIsCancelled_NacksWithRequeueWithoutErrorStrategy()
    {
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();
        var pipeline = new Mock<IInboundMessagePipeline>(MockBehavior.Strict);
        pipeline
            .Setup(x => x.HandleEventAsync(
                It.IsAny<MessageEnvelope<ClientCreatedEvent>>(),
                cancellationTokenSource.Token))
            .Returns(Task.FromCanceled(cancellationTokenSource.Token));
        await using var provider = CreateProvider(_ => pipeline.Object);
        var errorDependencies = CreateErrorStrategy(deadLetteringEnabled: true);
        var consumer = CreateConsumer(provider, errorDependencies.Strategy);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        channel
            .Setup(x => x.BasicNackAsync(1, false, true, CancellationToken.None))
            .Returns(ValueTask.CompletedTask);

        await consumer.ConsumeAsync(
            channel.Object,
            CreateSubscription(),
            CreateDelivery(new ClientCreatedEvent("Acme"), cancellationToken: cancellationTokenSource.Token),
            cancellationTokenSource.Token);

        pipeline.VerifyAll();
        channel.VerifyAll();
        errorDependencies.VerifyNoCalls();
    }

    [Fact]
    public async Task ConsumeAsync_WhenAcknowledgementFails_DoesNotInvokeErrorStrategy()
    {
        var pipeline = new Mock<IInboundMessagePipeline>(MockBehavior.Strict);
        pipeline
            .Setup(x => x.HandleEventAsync(
                It.IsAny<MessageEnvelope<ClientCreatedEvent>>(),
                CancellationToken.None))
            .Returns(Task.CompletedTask);
        await using var provider = CreateProvider(_ => pipeline.Object);
        var errorDependencies = CreateErrorStrategy(deadLetteringEnabled: true);
        var consumer = CreateConsumer(provider, errorDependencies.Strategy);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        var acknowledgementException = new InvalidOperationException("channel closed");
        channel
            .Setup(x => x.BasicAckAsync(1, false, CancellationToken.None))
            .Returns(ValueTask.FromException(acknowledgementException));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => consumer.ConsumeAsync(
                channel.Object,
                CreateSubscription(),
                CreateDelivery(new ClientCreatedEvent("Acme")),
                CancellationToken.None));

        Assert.Same(acknowledgementException, exception);
        pipeline.VerifyAll();
        channel.VerifyAll();
        errorDependencies.VerifyNoCalls();
    }

    private static ServiceProvider CreateProvider(
        Func<IServiceProvider, IInboundMessagePipeline> pipelineFactory)
    {
        var services = new ServiceCollection();
        services.AddScoped<IInboundMessagePipeline>(pipelineFactory);
        return services.BuildServiceProvider();
    }

    private static RabbitMqMessageConsumer CreateConsumer(
        ServiceProvider provider,
        RabbitMqConsumeErrorStrategy errorStrategy)
    {
        var catalog = new MessageDispatchCatalog(
        [
            MessageDispatchDescriptorFactory.CreateEvent<ClientCreatedEvent>()
        ]);

        return new RabbitMqMessageConsumer(
            provider.GetRequiredService<IServiceScopeFactory>(),
            new MessageJsonSerializer(),
            catalog,
            errorStrategy);
    }

    private static ErrorStrategyDependencies CreateErrorStrategy(bool deadLetteringEnabled)
    {
        var deadLetterPublisher = new Mock<IRabbitMqDeadLetterPublisher>(MockBehavior.Strict);
        var delayedRetryPublisher = new Mock<IRabbitMqDelayedRetryPublisher>(MockBehavior.Strict);
        var strategy = new RabbitMqConsumeErrorStrategy(
            deadLetterPublisher.Object,
            delayedRetryPublisher.Object,
            Options.Create(new RabbitMqConsumerOptions
            {
                DeadLetteringEnabled = deadLetteringEnabled
            }),
            NullLogger<RabbitMqConsumeErrorStrategy>.Instance);

        return new ErrorStrategyDependencies(
            strategy,
            deadLetterPublisher,
            delayedRetryPublisher);
    }

    private static RabbitMqSubscription CreateSubscription()
    {
        return new RabbitMqSubscription
        {
            ExchangeName = "events/clients",
            QueueName = "events/clients/subscription/test-service",
            ConsumerSettings = new RabbitMqConsumerSettings(1, 1, 1)
        };
    }

    private static BasicDeliverEventArgs CreateDelivery(
        ClientCreatedEvent message,
        MessageMetadata? metadata = null,
        ulong deliveryTag = 1,
        CancellationToken cancellationToken = default)
    {
        metadata ??= new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow);
        var properties = RabbitMqMessagePropertiesMapper.MapToBrokerProperties(
            typeof(ClientCreatedEvent),
            metadata);

        return CreateDelivery(message, properties, deliveryTag, cancellationToken);
    }

    private static BasicDeliverEventArgs CreateDelivery(
        ClientCreatedEvent message,
        IReadOnlyBasicProperties properties,
        ulong deliveryTag = 1,
        CancellationToken cancellationToken = default)
    {
        var body = new MessageJsonSerializer().SerializeToUtf8Bytes(message);

        return new BasicDeliverEventArgs(
            "consumer",
            deliveryTag,
            false,
            "events/clients",
            "client-created-event",
            properties,
            body,
            cancellationToken);
    }

    private sealed record ErrorStrategyDependencies(
        RabbitMqConsumeErrorStrategy Strategy,
        Mock<IRabbitMqDeadLetterPublisher> DeadLetterPublisher,
        Mock<IRabbitMqDelayedRetryPublisher> DelayedRetryPublisher)
    {
        public void VerifyNoCalls()
        {
            DeadLetterPublisher.VerifyNoOtherCalls();
            DelayedRetryPublisher.VerifyNoOtherCalls();
        }
    }

    private sealed class DisposableInboundMessagePipeline(Exception? exception = null)
        : IInboundMessagePipeline, IAsyncDisposable
    {
        public bool IsDisposed { get; private set; }

        public Task HandleEventAsync<TEvent>(
            MessageEnvelope<TEvent> envelope,
            CancellationToken token = default)
            where TEvent : class, IEvent
        {
            return exception is null
                ? Task.CompletedTask
                : Task.FromException(exception);
        }

        public Task HandleCommandAsync<TCommand>(
            MessageEnvelope<TCommand> envelope,
            CancellationToken token = default)
            where TCommand : class, ICommand
        {
            return exception is null
                ? Task.CompletedTask
                : Task.FromException(exception);
        }

        public ValueTask DisposeAsync()
        {
            IsDisposed = true;
            return ValueTask.CompletedTask;
        }
    }
}
