using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using EasyNetQ;
using EasyNetQ.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer;

public sealed class RabbitMqMessageConsumerTests
{
    [Fact]
    public async Task ConsumeAsync_MapsEnvelopeAndReturnsAckAfterPipelineSucceeds()
    {
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(2026, 11, 7, 15, 30, 0, TimeSpan.Zero);
        var body = new ClientCreatedEvent("Acme");
        var message = CreateMessage(body, messageId, correlationId, createdAt);
        var receivedInfo = CreateReceivedInfo();
        using var cancellationTokenSource = new CancellationTokenSource();
        MessageEnvelope<ClientCreatedEvent>? capturedEnvelope = null;
        var pipeline = new Mock<IInboundMessagePipeline>(MockBehavior.Strict);
        pipeline
            .Setup(x => x.HandleEventAsync(
                It.IsAny<MessageEnvelope<ClientCreatedEvent>>(),
                cancellationTokenSource.Token))
            .Callback<MessageEnvelope<ClientCreatedEvent>, CancellationToken>(
                (envelope, _) => capturedEnvelope = envelope)
            .Returns(Task.CompletedTask);
        await using var provider = CreateProvider(_ => pipeline.Object);
        var consumer = CreateConsumer(provider);

        var ackStrategy = await consumer.ConsumeAsync(
            message,
            receivedInfo,
            static (inboundPipeline, envelope, ct) =>
                inboundPipeline.HandleEventAsync(envelope, ct),
            cancellationTokenSource.Token);

        Assert.Same(AckStrategies.AckAsync, ackStrategy);
        Assert.NotNull(capturedEnvelope);
        Assert.Same(body, capturedEnvelope.Message);
        Assert.Equal(messageId, capturedEnvelope.Metadata.MessageId);
        Assert.Equal(correlationId, capturedEnvelope.Metadata.CorrelationId);
        Assert.Equal(createdAt, capturedEnvelope.Metadata.CreatedAt);
        pipeline.VerifyAll();
    }

    [Fact]
    public async Task ConsumeAsync_WhenPipelineThrows_PropagatesExceptionAndDisposesScope()
    {
        var expectedException = new InvalidOperationException("handler failed");
        var instances = new List<DisposableInboundMessagePipeline>();
        await using var provider = CreateProvider(_ =>
        {
            var pipeline = new DisposableInboundMessagePipeline(expectedException);
            instances.Add(pipeline);
            return pipeline;
        });
        var consumer = CreateConsumer(provider);
        var message = CreateMessage(
            new ClientCreatedEvent("Acme"),
            Guid.NewGuid(),
            null,
            DateTimeOffset.UtcNow);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => consumer.ConsumeAsync(
                message,
                CreateReceivedInfo(),
                static (pipeline, envelope, ct) =>
                    pipeline.HandleEventAsync(envelope, ct),
                CancellationToken.None));

        Assert.Same(expectedException, exception);
        Assert.True(Assert.Single(instances).IsDisposed);
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
        var consumer = CreateConsumer(provider);
        var message = CreateMessage(
            new ClientCreatedEvent("Acme"),
            Guid.NewGuid(),
            null,
            DateTimeOffset.UtcNow);

        await consumer.ConsumeAsync(
            message,
            CreateReceivedInfo(),
            static (pipeline, envelope, ct) =>
                pipeline.HandleEventAsync(envelope, ct),
            CancellationToken.None);
        await consumer.ConsumeAsync(
            message,
            CreateReceivedInfo(),
            static (pipeline, envelope, ct) =>
                pipeline.HandleEventAsync(envelope, ct),
            CancellationToken.None);

        Assert.Equal(2, instances.Count);
        Assert.NotSame(instances[0], instances[1]);
        Assert.All(instances, pipeline => Assert.True(pipeline.IsDisposed));
    }

    [Fact]
    public async Task ConsumeAsync_WithInvalidMetadata_DoesNotCreateScope()
    {
        var createdPipelines = 0;
        await using var provider = CreateProvider(_ =>
        {
            createdPipelines++;
            return new DisposableInboundMessagePipeline();
        });
        var consumer = CreateConsumer(provider);
        var properties = new MessageProperties
        {
            MessageId = "invalid",
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
        };
        var message = new Message<ClientCreatedEvent>(
            new ClientCreatedEvent("Acme"),
            properties);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => consumer.ConsumeAsync(
                message,
                CreateReceivedInfo(),
                static (pipeline, envelope, ct) =>
                    pipeline.HandleEventAsync(envelope, ct),
                CancellationToken.None));

        Assert.Equal(0, createdPipelines);
    }

    [Fact]
    public async Task ConsumeAsync_WhenMessageIsNull_Throws()
    {
        var pipeline = new Mock<IInboundMessagePipeline>(MockBehavior.Strict);
        await using var provider = CreateProvider(_ => pipeline.Object);
        var consumer = CreateConsumer(provider);

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => consumer.ConsumeAsync<ClientCreatedEvent>(
                null!,
                CreateReceivedInfo(),
                static (inboundPipeline, envelope, ct) =>
                    inboundPipeline.HandleEventAsync(envelope, ct),
                CancellationToken.None));

        pipeline.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task ConsumeAsync_WhenHandleIsNull_Throws()
    {
        var pipeline = new Mock<IInboundMessagePipeline>(MockBehavior.Strict);
        await using var provider = CreateProvider(_ => pipeline.Object);
        var consumer = CreateConsumer(provider);
        var message = CreateMessage(
            new ClientCreatedEvent("Acme"),
            Guid.NewGuid(),
            null,
            DateTimeOffset.UtcNow);

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => consumer.ConsumeAsync(
                message,
                CreateReceivedInfo(),
                null!,
                CancellationToken.None));

        pipeline.VerifyNoOtherCalls();
    }

    private static ServiceProvider CreateProvider(Func<IServiceProvider, IInboundMessagePipeline> pipelineFactory)
    {
        var services = new ServiceCollection();
        services.AddScoped(pipelineFactory);

        return services.BuildServiceProvider();
    }

    private static RabbitMqMessageConsumer CreateConsumer(ServiceProvider provider)
    {
        return new RabbitMqMessageConsumer(provider.GetRequiredService<IServiceScopeFactory>());
    }

    private static Message<ClientCreatedEvent> CreateMessage(ClientCreatedEvent body, Guid messageId, Guid? correlationId, DateTimeOffset createdAt)
    {
        var properties = new MessageProperties
        {
            MessageId = messageId.ToString("D"),
            CorrelationId = correlationId?.ToString("D")!,
            Timestamp = createdAt.ToUnixTimeSeconds()
        };

        return new Message<ClientCreatedEvent>(body, properties);
    }

    private static MessageReceivedInfo CreateReceivedInfo()
    {
        return new MessageReceivedInfo(
            "consumer",
            1,
            false,
            "events/clients",
            "client-created-event",
            "events/clients/subscription/test-service");
    }

    private sealed class DisposableInboundMessagePipeline( Exception? exception = null) : IInboundMessagePipeline, IAsyncDisposable
    {
        public bool IsDisposed { get; private set; }

        public Task HandleEventAsync<TEvent>(MessageEnvelope<TEvent> envelope, CancellationToken token = default)
            where TEvent : class, IEvent
        {
            return exception is null
                ? Task.CompletedTask
                : Task.FromException(exception);
        }

        public Task HandleCommandAsync<TCommand>(MessageEnvelope<TCommand> envelope, CancellationToken token = default)
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