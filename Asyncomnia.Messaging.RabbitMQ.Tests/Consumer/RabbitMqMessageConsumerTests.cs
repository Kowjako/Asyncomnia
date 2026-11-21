using System.Collections.Frozen;
using Asyncomnia.Messaging.Dispatching;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Internal.Serialization;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.Consumer;
using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Unhandled;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Serialization;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Consumer;

public sealed class RabbitMqMessageConsumerTests
{
    [Fact]
    public async Task ConsumeAsync_WhenPipelineSucceeds_PassesInboundContextAndAcknowledgesMessage()
    {
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(2026, 11, 7, 15, 30, 0, TimeSpan.Zero);
        var body = new ClientCreatedEvent("Acme");
        InboundMessageContext? capturedContext = null;
        var pipeline = new Mock<IInboundMessagePipeline>(MockBehavior.Strict);
        pipeline
            .Setup(x => x.HandleEventAsync<ClientCreatedEvent>(
                It.IsAny<InboundMessageContext>(),
                CancellationToken.None))
            .Callback<InboundMessageContext, CancellationToken>(
                (context, _) => capturedContext = context)
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

        Assert.NotNull(capturedContext);
        Assert.Equal("events/clients/subscription/test-service", capturedContext.ConsumerId);
        Assert.Equal(delivery.BasicProperties.Type, capturedContext.ContractType);
        Assert.Equal(body, capturedContext.Message);
        Assert.Equal(messageId, capturedContext.Metadata.MessageId);
        Assert.Equal(correlationId, capturedContext.Metadata.CorrelationId);
        Assert.Equal(createdAt, capturedContext.Metadata.CreatedAt);
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
    public async Task ConsumeAsync_WhenMessageTypeIsNotConfiguredForSubscription_MovesMessageToUnhandledAndAcknowledges()
    {
        var createdPipelines = 0;
        await using var provider = CreateProvider(_ =>
        {
            createdPipelines++;
            return new DisposableInboundMessagePipeline();
        });
        var errorDependencies = CreateErrorStrategy(
            deadLetteringEnabled: true);
        var unhandledDependencies = CreateUnhandledStrategy();
        BasicProperties? publishedProperties = null;
        unhandledDependencies.Publisher
            .Setup(x => x.PublishAsync(
                "asyncomnia/unhandled",
                "unhandled/events/clients/subscription/test-service",
                true,
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                CancellationToken.None))
            .Callback<string, string, bool, BasicProperties,
                ReadOnlyMemory<byte>, CancellationToken>(
                (_, _, _, properties, _, _) =>
                    publishedProperties = properties)
            .Returns(Task.CompletedTask);
        var consumer = CreateConsumer(
            provider,
            errorDependencies.Strategy,
            unhandledDependencies.Strategy);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        channel
            .Setup(x => x.BasicAckAsync(
                1,
                false,
                CancellationToken.None))
            .Returns(ValueTask.CompletedTask);
        var subscription = CreateSubscription(
            typeof(ClientContractSignedEvent));

        await consumer.ConsumeAsync(
            channel.Object,
            subscription,
            CreateDelivery(new ClientCreatedEvent("Acme")),
            CancellationToken.None);

        Assert.Equal(0, createdPipelines);
        Assert.NotNull(publishedProperties);
        Assert.Equal(
            UnhandledMessageReason.NotConfiguredForSubscription,
            publishedProperties!.Headers![
                "x-asyncomnia-unhandled-reason"]);
        errorDependencies.VerifyNoCalls();
        unhandledDependencies.Publisher.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task ConsumeAsync_WhenContractIsUnknown_MovesMessageToUnhandledAndAcknowledges()
    {
        var createdPipelines = 0;
        await using var provider = CreateProvider(_ =>
        {
            createdPipelines++;
            return new DisposableInboundMessagePipeline();
        });
        var errorDependencies = CreateErrorStrategy(
            deadLetteringEnabled: true);
        var unhandledDependencies = CreateUnhandledStrategy();
        BasicProperties? publishedProperties = null;
        unhandledDependencies.Publisher
            .Setup(x => x.PublishAsync(
                "asyncomnia/unhandled",
                "unhandled/events/clients/subscription/test-service",
                true,
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                CancellationToken.None))
            .Callback<string, string, bool, BasicProperties,
                ReadOnlyMemory<byte>, CancellationToken>(
                (_, _, _, properties, _, _) =>
                    publishedProperties = properties)
            .Returns(Task.CompletedTask);
        var consumer = CreateConsumer(
            provider,
            errorDependencies.Strategy,
            unhandledDependencies.Strategy);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        channel
            .Setup(x => x.BasicAckAsync(
                1,
                false,
                CancellationToken.None))
            .Returns(ValueTask.CompletedTask);
        var originalProperties =
            RabbitMqMessagePropertiesMapper.MapToBrokerProperties(
                typeof(ClientCreatedEvent),
                new MessageMetadata(
                    Guid.NewGuid(),
                    DateTimeOffset.UtcNow));
        var properties = new BasicProperties(originalProperties)
        {
            Type = "Contracts.ClientDeleted, Contracts"
        };

        await consumer.ConsumeAsync(
            channel.Object,
            CreateSubscription(),
            CreateDelivery(new ClientCreatedEvent("Acme"), properties),
            CancellationToken.None);

        Assert.Equal(0, createdPipelines);
        Assert.NotNull(publishedProperties);
        Assert.Equal(
            properties.Type,
            publishedProperties!.Type);
        Assert.Equal(
            UnhandledMessageReason.UnknownContract,
            publishedProperties.Headers![
                "x-asyncomnia-unhandled-reason"]);
        errorDependencies.VerifyNoCalls();
        unhandledDependencies.Publisher.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task ConsumeAsync_WhenContractTypeIsMissing_MovesMessageToUnhandledAndAcknowledges()
    {
        var createdPipelines = 0;
        await using var provider = CreateProvider(_ =>
        {
            createdPipelines++;
            return new DisposableInboundMessagePipeline();
        });
        var errorDependencies = CreateErrorStrategy(
            deadLetteringEnabled: true);
        var unhandledDependencies = CreateUnhandledStrategy();
        BasicProperties? publishedProperties = null;
        unhandledDependencies.Publisher
            .Setup(x => x.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                true,
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                CancellationToken.None))
            .Callback<string, string, bool, BasicProperties,
                ReadOnlyMemory<byte>, CancellationToken>(
                (_, _, _, properties, _, _) =>
                    publishedProperties = properties)
            .Returns(Task.CompletedTask);
        var consumer = CreateConsumer(
            provider,
            errorDependencies.Strategy,
            unhandledDependencies.Strategy);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        channel
            .Setup(x => x.BasicAckAsync(
                1,
                false,
                CancellationToken.None))
            .Returns(ValueTask.CompletedTask);
        var originalProperties =
            RabbitMqMessagePropertiesMapper.MapToBrokerProperties(
                typeof(ClientCreatedEvent),
                new MessageMetadata(
                    Guid.NewGuid(),
                    DateTimeOffset.UtcNow));
        var properties = new BasicProperties(originalProperties)
        {
            Type = null
        };

        await consumer.ConsumeAsync(
            channel.Object,
            CreateSubscription(),
            CreateDelivery(new ClientCreatedEvent("Acme"), properties),
            CancellationToken.None);

        Assert.Equal(0, createdPipelines);
        Assert.NotNull(publishedProperties);
        Assert.Equal(
            UnhandledMessageReason.MissingContractType,
            publishedProperties!.Headers![
                "x-asyncomnia-unhandled-reason"]);
        errorDependencies.VerifyNoCalls();
        unhandledDependencies.Publisher.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task ConsumeAsync_WhenUnhandledPublishFails_NacksWithRequeueWithoutErrorStrategy()
    {
        var createdPipelines = 0;
        await using var provider = CreateProvider(_ =>
        {
            createdPipelines++;
            return new DisposableInboundMessagePipeline();
        });
        var errorDependencies = CreateErrorStrategy(
            deadLetteringEnabled: true);
        var unhandledDependencies = CreateUnhandledStrategy();
        unhandledDependencies.Publisher
            .Setup(x => x.PublishAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                true,
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                CancellationToken.None))
            .ThrowsAsync(new InvalidOperationException("broker unavailable"));
        var consumer = CreateConsumer(
            provider,
            errorDependencies.Strategy,
            unhandledDependencies.Strategy);
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        channel
            .Setup(x => x.BasicNackAsync(
                1,
                false,
                true,
                CancellationToken.None))
            .Returns(ValueTask.CompletedTask);
        var originalProperties =
            RabbitMqMessagePropertiesMapper.MapToBrokerProperties(
                typeof(ClientCreatedEvent),
                new MessageMetadata(
                    Guid.NewGuid(),
                    DateTimeOffset.UtcNow));
        var properties = new BasicProperties(originalProperties)
        {
            Type = "Contracts.ClientDeleted, Contracts"
        };

        await consumer.ConsumeAsync(
            channel.Object,
            CreateSubscription(),
            CreateDelivery(new ClientCreatedEvent("Acme"), properties),
            CancellationToken.None);

        Assert.Equal(0, createdPipelines);
        errorDependencies.VerifyNoCalls();
        unhandledDependencies.Publisher.VerifyAll();
        channel.VerifyAll();
    }

    [Fact]
    public async Task ConsumeAsync_WhenProcessingIsCancelled_NacksWithRequeueWithoutErrorStrategy()
    {
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();
        var pipeline = new Mock<IInboundMessagePipeline>(MockBehavior.Strict);
        pipeline
            .Setup(x => x.HandleEventAsync<ClientCreatedEvent>(
                It.IsAny<InboundMessageContext>(),
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
            .Setup(x => x.HandleEventAsync<ClientCreatedEvent>(
                It.IsAny<InboundMessageContext>(),
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

    [Fact]
    public async Task ConsumeAsync_WhenMessagesShareChannel_SerializesAcknowledgements()
    {
        var processedMessages = 0;
        var bothMessagesProcessed = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = new Mock<IInboundMessagePipeline>(MockBehavior.Strict);
        pipeline
            .Setup(x => x.HandleEventAsync<ClientCreatedEvent>(
                It.IsAny<InboundMessageContext>(),
                CancellationToken.None))
            .Callback<InboundMessageContext, CancellationToken>((_, _) =>
            {
                if (Interlocked.Increment(ref processedMessages) == 2)
                {
                    bothMessagesProcessed.TrySetResult(true);
                }
            })
            .Returns(Task.CompletedTask);
        await using var provider = CreateProvider(_ => pipeline.Object);
        var errorDependencies = CreateErrorStrategy(deadLetteringEnabled: false);
        var consumer = CreateConsumer(provider, errorDependencies.Strategy);
        var firstAcknowledgementStarted = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstAcknowledgement = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var acknowledgementCalls = 0;
        var channel = new Mock<IChannel>(MockBehavior.Strict);
        channel
            .Setup(x => x.BasicAckAsync(
                It.IsAny<ulong>(),
                false,
                CancellationToken.None))
            .Returns((ulong deliveryTag, bool multiple, CancellationToken token) =>
            {
                var call = Interlocked.Increment(ref acknowledgementCalls);

                if (call == 1)
                {
                    firstAcknowledgementStarted.TrySetResult(true);
                    return new ValueTask(releaseFirstAcknowledgement.Task);
                }

                return ValueTask.CompletedTask;
            });

        var firstConsume = consumer.ConsumeAsync(
            channel.Object,
            CreateSubscription(),
            CreateDelivery(new ClientCreatedEvent("First"), deliveryTag: 1),
            CancellationToken.None);
        Task? secondConsume = null;

        try
        {
            await firstAcknowledgementStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

            secondConsume = consumer.ConsumeAsync(
                channel.Object,
                CreateSubscription(),
                CreateDelivery(new ClientCreatedEvent("Second"), deliveryTag: 2),
                CancellationToken.None);

            await bothMessagesProcessed.Task.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Equal(1, Volatile.Read(ref acknowledgementCalls));
            Assert.False(secondConsume.IsCompleted);
        }
        finally
        {
            releaseFirstAcknowledgement.TrySetResult(true);
            await firstConsume;

            if (secondConsume is not null)
            {
                await secondConsume;
            }
        }

        Assert.Equal(2, acknowledgementCalls);
        pipeline.Verify(
            x => x.HandleEventAsync<ClientCreatedEvent>(
                It.IsAny<InboundMessageContext>(),
                CancellationToken.None),
            Times.Exactly(2));
        channel.Verify(
            x => x.BasicAckAsync(
                It.IsAny<ulong>(),
                false,
                CancellationToken.None),
            Times.Exactly(2));
        errorDependencies.VerifyNoCalls();
    }

    [Fact]
    public async Task ConsumeAsync_WhenMessagesUseDifferentChannels_DoesNotSerializeAcknowledgements()
    {
        var pipeline = new Mock<IInboundMessagePipeline>(MockBehavior.Strict);
        pipeline
            .Setup(x => x.HandleEventAsync<ClientCreatedEvent>(
                It.IsAny<InboundMessageContext>(),
                CancellationToken.None))
            .Returns(Task.CompletedTask);
        await using var provider = CreateProvider(_ => pipeline.Object);
        var errorDependencies = CreateErrorStrategy(deadLetteringEnabled: false);
        var consumer = CreateConsumer(provider, errorDependencies.Strategy);
        var firstAcknowledgementStarted = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstAcknowledgement = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var secondAcknowledgementStarted = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var firstChannel = new Mock<IChannel>(MockBehavior.Strict);
        firstChannel
            .Setup(x => x.BasicAckAsync(1, false, CancellationToken.None))
            .Returns(() =>
            {
                firstAcknowledgementStarted.TrySetResult(true);
                return new ValueTask(releaseFirstAcknowledgement.Task);
            });
        var secondChannel = new Mock<IChannel>(MockBehavior.Strict);
        secondChannel
            .Setup(x => x.BasicAckAsync(2, false, CancellationToken.None))
            .Returns(() =>
            {
                secondAcknowledgementStarted.TrySetResult(true);
                return ValueTask.CompletedTask;
            });

        var firstConsume = consumer.ConsumeAsync(
            firstChannel.Object,
            CreateSubscription(),
            CreateDelivery(new ClientCreatedEvent("First"), deliveryTag: 1),
            CancellationToken.None);
        Task? secondConsume = null;

        try
        {
            await firstAcknowledgementStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

            secondConsume = consumer.ConsumeAsync(
                secondChannel.Object,
                CreateSubscription(),
                CreateDelivery(new ClientCreatedEvent("Second"), deliveryTag: 2),
                CancellationToken.None);

            await secondAcknowledgementStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await secondConsume.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.False(firstConsume.IsCompleted);
        }
        finally
        {
            releaseFirstAcknowledgement.TrySetResult(true);
            await firstConsume;

            if (secondConsume is not null)
            {
                await secondConsume;
            }
        }

        pipeline.Verify(
            x => x.HandleEventAsync<ClientCreatedEvent>(
                It.IsAny<InboundMessageContext>(),
                CancellationToken.None),
            Times.Exactly(2));
        firstChannel.VerifyAll();
        secondChannel.VerifyAll();
        errorDependencies.VerifyNoCalls();
    }

    private static ServiceProvider CreateProvider(
        Func<IServiceProvider, IInboundMessagePipeline> pipelineFactory)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMessageDispatchCatalog>(
            CreateDispatchCatalog());
        services.AddScoped<IInboundMessagePipeline>(pipelineFactory);
        return services.BuildServiceProvider();
    }

    private static RabbitMqMessageConsumer CreateConsumer(
        ServiceProvider provider,
        RabbitMqConsumeErrorStrategy errorStrategy,
        RabbitMqUnhandledMessageStrategy? unhandledStrategy = null)
    {
        unhandledStrategy ??= new RabbitMqUnhandledMessageStrategy(
            NullLogger<RabbitMqUnhandledMessageStrategy>.Instance,
            new RabbitMqUnhandledRouteResolver(),
            new Mock<IRabbitMqPublisher>(MockBehavior.Strict).Object);

        return new RabbitMqMessageConsumer(
            provider.GetRequiredService<IServiceScopeFactory>(),
            new MessageJsonSerializer(),
            provider.GetRequiredService<IMessageDispatchCatalog>(),
            errorStrategy,
            unhandledStrategy);
    }

    private static MessageDispatchCatalog CreateDispatchCatalog()
    {
        return new MessageDispatchCatalog(
        [
            MessageDispatchDescriptorFactory
                .CreateEvent<ClientCreatedEvent>()
        ]);
    }

    private static ErrorStrategyDependencies CreateErrorStrategy(
        bool deadLetteringEnabled,
        ILogger<RabbitMqConsumeErrorStrategy>? logger = null)
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
            logger ?? NullLogger<RabbitMqConsumeErrorStrategy>.Instance);

        return new ErrorStrategyDependencies(
            strategy,
            deadLetterPublisher,
            delayedRetryPublisher);
    }

    private static UnhandledStrategyDependencies CreateUnhandledStrategy()
    {
        var publisher = new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var strategy = new RabbitMqUnhandledMessageStrategy(
            NullLogger<RabbitMqUnhandledMessageStrategy>.Instance,
            new RabbitMqUnhandledRouteResolver(),
            publisher.Object);

        return new UnhandledStrategyDependencies(strategy, publisher);
    }

    private static RabbitMqSubscription CreateSubscription(
        Type? messageType = null)
    {
        return new RabbitMqSubscription
        {
            ExchangeName = "events/clients",
            QueueName = "events/clients/subscription/test-service",
            Bindings = new Dictionary<Type, string>
            {
                [messageType ?? typeof(ClientCreatedEvent)] = "client-created-event"
            }.ToFrozenDictionary(),
            Settings = new RabbitMqSubscriptionSettings(
                RabbitMqQueueType.Quorum,
                false,
                1,
                1)
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

    private sealed record UnhandledStrategyDependencies(
        RabbitMqUnhandledMessageStrategy Strategy,
        Mock<IRabbitMqPublisher> Publisher);

    private sealed class DisposableInboundMessagePipeline(Exception? exception = null)
        : IInboundMessagePipeline, IAsyncDisposable
    {
        public bool IsDisposed { get; private set; }

        public Task HandleEventAsync<TEvent>(
            InboundMessageContext context,
            CancellationToken token = default)
            where TEvent : class, IEvent
        {
            return exception is null
                ? Task.CompletedTask
                : Task.FromException(exception);
        }

        public Task HandleCommandAsync<TCommand>(
            InboundMessageContext context,
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
