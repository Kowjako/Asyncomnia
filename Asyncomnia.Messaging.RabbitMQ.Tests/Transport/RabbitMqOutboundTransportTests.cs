using Asyncomnia.Messaging.Internal.Serialization;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Publishing;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Serialization;
using Asyncomnia.Messaging.RabbitMQ.Infrastructure.Amqp.Topology;
using Asyncomnia.Messaging.RabbitMQ.Routing;
using Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;
using Asyncomnia.Messaging.RabbitMQ.Transport;
using Moq;
using RabbitMQ.Client;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.Transport;

public sealed class RabbitMqOutboundTransportTests
{
    [Fact]
    public async Task DispatchAsync_ForEvent_DeclaresExchangeAndPublishesSerializedMessage()
    {
        var exchangeCreator =
            new Mock<IRabbitMqExchangeCreator>(MockBehavior.Strict);
        var publisher =
            new Mock<IRabbitMqPublisher>(MockBehavior.Strict);

        var transport = CreateTransport(exchangeCreator, publisher);
        var @event = new ClientCreatedEvent("Acme");
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var createdAt = new DateTimeOffset(
            2026,
            11,
            6,
            12,
            30,
            45,
            TimeSpan.Zero);
        var metadata = new MessageMetadata(messageId, createdAt)
        {
            CorrelationId = correlationId
        };

        using var cancellationSource = new CancellationTokenSource();
        var token = cancellationSource.Token;
        BasicProperties? publishedProperties = null;
        ReadOnlyMemory<byte> publishedBody = default;
        var sequence = new MockSequence();
        exchangeCreator
            .InSequence(sequence)
            .Setup(x => x.EnsureDeclaredAsync(
                "events/clients",
                token))
            .Returns(Task.CompletedTask);
        publisher
            .InSequence(sequence)
            .Setup(x => x.PublishAsync(
                "events/clients",
                "client-created-event",
                false,
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                token))
            .Callback<string, string, bool, BasicProperties,
                ReadOnlyMemory<byte>, CancellationToken>(
                (_, _, _, properties, body, _) =>
                {
                    publishedProperties = properties;
                    publishedBody = body;
                })
            .Returns(Task.CompletedTask);

        await transport.DispatchAsync(@event, metadata, token);

        Assert.Equal(
            @event,
            new MessageJsonSerializer().Deserialize(
                publishedBody,
                typeof(ClientCreatedEvent)));
        Assert.NotNull(publishedProperties);
        Assert.Equal(
            RabbitMqMessagePropertiesMapper.JsonContentType,
            publishedProperties.ContentType);
        Assert.Equal(
            RabbitMqMessagePropertiesMapper.Utf8ContentEncoding,
            publishedProperties.ContentEncoding);
        Assert.Equal(
            "Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles." +
            "ClientCreatedEvent, Asyncomnia.Messaging.RabbitMQ.Tests",
            publishedProperties.Type);
        Assert.Equal(
            messageId.ToString("D"),
            publishedProperties.MessageId);
        Assert.Equal(
            correlationId.ToString("D"),
            publishedProperties.CorrelationId);
        Assert.Equal(
            createdAt.ToUnixTimeSeconds(),
            publishedProperties.Timestamp.UnixTime);
        Assert.Equal(
            DeliveryModes.Persistent,
            publishedProperties.DeliveryMode);
        exchangeCreator.VerifyAll();
        publisher.VerifyAll();
    }

    [Fact]
    public async Task DispatchAsync_ForCommand_PublishesAsMandatoryAndUsesMessageIdAsCorrelationFallback()
    {
        var exchangeCreator =
            new Mock<IRabbitMqExchangeCreator>(MockBehavior.Strict);
        var publisher =
            new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var transport = CreateTransport(exchangeCreator, publisher);
        var command = new StartLongWorkJobCommand(Guid.NewGuid());
        var messageId = Guid.NewGuid();
        var metadata = new MessageMetadata(
            messageId,
            DateTimeOffset.UtcNow);
        BasicProperties? publishedProperties = null;
        ReadOnlyMemory<byte> publishedBody = default;
        exchangeCreator
            .Setup(x => x.EnsureDeclaredAsync(
                "commands/long-work-job",
                CancellationToken.None))
            .Returns(Task.CompletedTask);
        publisher
            .Setup(x => x.PublishAsync(
                "commands/long-work-job",
                "start-long-work-job-command",
                true,
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                CancellationToken.None))
            .Callback<string, string, bool, BasicProperties,
                ReadOnlyMemory<byte>, CancellationToken>(
                (_, _, _, properties, body, _) =>
                {
                    publishedProperties = properties;
                    publishedBody = body;
                })
            .Returns(Task.CompletedTask);

        await transport.DispatchAsync(command, metadata);

        Assert.Equal(
            command,
            new MessageJsonSerializer().Deserialize(
                publishedBody,
                typeof(StartLongWorkJobCommand)));
        Assert.NotNull(publishedProperties);
        Assert.Equal(
            messageId.ToString("D"),
            publishedProperties.CorrelationId);
        exchangeCreator.VerifyAll();
        publisher.VerifyAll();
    }

    [Fact]
    public async Task DispatchAsync_WhenExchangeDeclarationFails_DoesNotPublishAndPropagatesException()
    {
        var exchangeCreator =
            new Mock<IRabbitMqExchangeCreator>(MockBehavior.Strict);
        var publisher =
            new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var transport = CreateTransport(exchangeCreator, publisher);
        var declarationException =
            new InvalidOperationException("declaration failed");
        exchangeCreator
            .Setup(x => x.EnsureDeclaredAsync(
                "events/clients",
                CancellationToken.None))
            .ThrowsAsync(declarationException);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => transport.DispatchAsync(
                new ClientCreatedEvent("Acme"),
                CreateMetadata()));

        Assert.Same(declarationException, exception);
        exchangeCreator.VerifyAll();
        publisher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task DispatchAsync_WhenPublicationFails_PropagatesException()
    {
        var exchangeCreator =
            new Mock<IRabbitMqExchangeCreator>(MockBehavior.Strict);
        var publisher =
            new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var transport = CreateTransport(exchangeCreator, publisher);
        var publicationException =
            new InvalidOperationException("publication failed");
        exchangeCreator
            .Setup(x => x.EnsureDeclaredAsync(
                "events/clients",
                CancellationToken.None))
            .Returns(Task.CompletedTask);
        publisher
            .Setup(x => x.PublishAsync(
                "events/clients",
                "client-created-event",
                false,
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                CancellationToken.None))
            .ThrowsAsync(publicationException);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => transport.DispatchAsync(
                new ClientCreatedEvent("Acme"),
                CreateMetadata()));

        Assert.Same(publicationException, exception);
        exchangeCreator.VerifyAll();
        publisher.VerifyAll();
    }

    [Fact]
    public async Task DispatchAsync_WithNullArguments_ThrowsBeforeUsingTransportDependencies()
    {
        var exchangeCreator =
            new Mock<IRabbitMqExchangeCreator>(MockBehavior.Strict);
        var publisher =
            new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var transport = CreateTransport(exchangeCreator, publisher);

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => transport.DispatchAsync(
                null!,
                CreateMetadata()));
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => transport.DispatchAsync(
                new ClientCreatedEvent("Acme"),
                null!));

        exchangeCreator.VerifyNoOtherCalls();
        publisher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task DispatchAsync_WhenCancellationWasAlreadyRequested_DoesNotUseTransportDependencies()
    {
        var exchangeCreator =
            new Mock<IRabbitMqExchangeCreator>(MockBehavior.Strict);
        var publisher =
            new Mock<IRabbitMqPublisher>(MockBehavior.Strict);
        var transport = CreateTransport(exchangeCreator, publisher);
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => transport.DispatchAsync(
                new ClientCreatedEvent("Acme"),
                CreateMetadata(),
                cancellationSource.Token));

        exchangeCreator.VerifyNoOtherCalls();
        publisher.VerifyNoOtherCalls();
    }

    [Fact]
    public void Constructor_WithNullDependencies_Throws()
    {
        var routeResolver = new MessageRouteResolver();
        var serializer = new MessageJsonSerializer();
        var exchangeCreator =
            new Mock<IRabbitMqExchangeCreator>(MockBehavior.Strict);
        var publisher =
            new Mock<IRabbitMqPublisher>(MockBehavior.Strict);

        var routeResolverException = Assert.Throws<ArgumentNullException>(
            () => new RabbitMqOutboundTransport(
                null!,
                serializer,
                exchangeCreator.Object,
                publisher.Object));
        var serializerException = Assert.Throws<ArgumentNullException>(
            () => new RabbitMqOutboundTransport(
                routeResolver,
                null!,
                exchangeCreator.Object,
                publisher.Object));
        var exchangeCreatorException = Assert.Throws<ArgumentNullException>(
            () => new RabbitMqOutboundTransport(
                routeResolver,
                serializer,
                null!,
                publisher.Object));
        var publisherException = Assert.Throws<ArgumentNullException>(
            () => new RabbitMqOutboundTransport(
                routeResolver,
                serializer,
                exchangeCreator.Object,
                null!));

        Assert.Equal("msgRouteResolver", routeResolverException.ParamName);
        Assert.Equal("msgSerializer", serializerException.ParamName);
        Assert.Equal("exchangeCreator", exchangeCreatorException.ParamName);
        Assert.Equal("rabbitMqPublisher", publisherException.ParamName);
    }

    private static RabbitMqOutboundTransport CreateTransport(Mock<IRabbitMqExchangeCreator> exchangeCreator, Mock<IRabbitMqPublisher> publisher)
    {
        return new RabbitMqOutboundTransport(
            new MessageRouteResolver(),
            new MessageJsonSerializer(),
            exchangeCreator.Object,
            publisher.Object);
    }

    private static MessageMetadata CreateMetadata()
    {
        return new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow);
    }
}
