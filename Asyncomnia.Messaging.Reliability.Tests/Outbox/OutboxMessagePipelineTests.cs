using System.Text.Json;
using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace Asyncomnia.Messaging.Reliability.Tests.Outbox;

public sealed class OutboxMessagePipelineTests
{
    [Fact]
    public async Task PublishAsync_WritesSerializedEventWithEnvelopeMetadata()
    {
        var utcNow = new DateTimeOffset(2026, 11, 5, 12, 0, 0, TimeSpan.Zero);
        var correlationId = Guid.NewGuid();
        OutboxMessage? writtenMessage = null;
        CancellationToken writtenToken = default;
        var writer = CreateWriter((message, token) =>
        {
            writtenMessage = message;
            writtenToken = token;
        });
        await using var provider = CreateProvider(utcNow, writer.Object);
        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();
        var @event = new TestOutboxEvent("event-value");
        using var cancellationTokenSource = new CancellationTokenSource();

        await publisher.PublishAsync(
            @event,
            new MessageDispatchOptions { CorrelationId = correlationId },
            cancellationTokenSource.Token);

        var outboxMessage = Assert.IsType<OutboxMessage>(writtenMessage);
        Assert.NotEqual(Guid.Empty, outboxMessage.MessageId);
        Assert.Equal(GetContractType(typeof(TestOutboxEvent)), outboxMessage.ContractType);
        Assert.Equal(correlationId, outboxMessage.CorrelationId);
        Assert.Equal(utcNow, outboxMessage.CreatedAt);
        Assert.Equal(cancellationTokenSource.Token, writtenToken);

        using var json = JsonDocument.Parse(outboxMessage.MessageJson);
        Assert.Equal(@event.Value, json.RootElement.GetProperty("value").GetString());
        writer.Verify(
            x => x.WriteAsync(It.IsAny<OutboxMessage>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task SendAsync_WritesSerializedCommand()
    {
        OutboxMessage? writtenMessage = null;
        var writer = CreateWriter((message, _) => writtenMessage = message);
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, writer.Object);
        await using var scope = provider.CreateAsyncScope();
        var sender = scope.ServiceProvider.GetRequiredService<ICommandSender>();
        var command = new TestOutboxCommand("command-value");

        await sender.SendAsync(command);

        var outboxMessage = Assert.IsType<OutboxMessage>(writtenMessage);
        Assert.Equal(GetContractType(typeof(TestOutboxCommand)), outboxMessage.ContractType);

        using var json = JsonDocument.Parse(outboxMessage.MessageJson);
        Assert.Equal(command.Value, json.RootElement.GetProperty("value").GetString());
        writer.Verify(
            x => x.WriteAsync(It.IsAny<OutboxMessage>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task PublishAsync_WhenVariableUsesBaseType_StoresRuntimeContractType()
    {
        OutboxMessage? writtenMessage = null;
        var writer = CreateWriter((message, _) => writtenMessage = message);
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, writer.Object);
        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();
        TestBaseEvent @event = new TestDerivedEvent("derived-value");

        await publisher.PublishAsync(@event);

        var outboxMessage = Assert.IsType<OutboxMessage>(writtenMessage);
        Assert.Equal(GetContractType(typeof(TestDerivedEvent)), outboxMessage.ContractType);

        using var json = JsonDocument.Parse(outboxMessage.MessageJson);
        Assert.Equal("derived-value", json.RootElement.GetProperty("value").GetString());
    }

    [Fact]
    public async Task PublishAsync_WhenWriterThrows_PropagatesException()
    {
        var expectedException = new InvalidOperationException("Write failed");
        var writer = new Mock<IOutboxWriter>(MockBehavior.Strict);
        writer
            .Setup(x => x.WriteAsync(It.IsAny<OutboxMessage>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(expectedException);
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, writer.Object);
        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => publisher.PublishAsync(new TestOutboxEvent("value")));

        Assert.Same(expectedException, exception);
    }

    private static Mock<IOutboxWriter> CreateWriter(Action<OutboxMessage, CancellationToken> onWrite)
    {
        var writer = new Mock<IOutboxWriter>(MockBehavior.Strict);
        writer
            .Setup(x => x.WriteAsync(It.IsAny<OutboxMessage>(), It.IsAny<CancellationToken>()))
            .Callback(onWrite)
            .Returns(Task.CompletedTask);

        return writer;
    }

    private static ServiceProvider CreateProvider(DateTimeOffset utcNow, IOutboxWriter writer)
    {
        var services = new ServiceCollection();

        services.AddSingleton<TimeProvider>(new FixedTimeProvider(utcNow));
        services.AddSingleton(writer);
        services.AddAsyncomniaMessaging().UseOutbox();

        return services.BuildServiceProvider();
    }

    private static string GetContractType(Type messageType)
    {
        return $"{messageType.FullName}, {messageType.Assembly.GetName().Name}";
    }
}
