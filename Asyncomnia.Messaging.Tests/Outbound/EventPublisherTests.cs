using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Outbound;
using Asyncomnia.Messaging.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Tests.Outbound;

public sealed class EventPublisherTests
{
    [Fact]
    public async Task PublishAsync_CreatesEnvelopeWithMessageIdAndTimestamp()
    {
        var utcNow = new DateTimeOffset(2026, 11, 5, 12, 0, 0, TimeSpan.Zero);
        await using var provider = CreateProvider(utcNow, out var pipeline);
        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();
        var message = new TestEvent("value");

        await publisher.PublishAsync(message);

        var envelope = Assert.IsType<MessageEnvelope<TestEvent>>(pipeline.PublishedEnvelope);
        Assert.Same(message, envelope.Message);
        Assert.NotEqual(Guid.Empty, envelope.Metadata.MessageId);
        Assert.Equal(utcNow, envelope.Metadata.CreatedAt);
        Assert.Null(envelope.Metadata.CorrelationId);
        Assert.Equal(1, pipeline.PublishCalls);
        Assert.Equal(0, pipeline.SendCalls);
    }

    [Fact]
    public async Task PublishAsync_WithCorrelationId_AddsCorrelationIdToMetadata()
    {
        var correlationId = Guid.NewGuid();
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, out var pipeline);
        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();

        await publisher.PublishAsync(
            new TestEvent("value"),
            new MessageDispatchOptions { CorrelationId = correlationId });

        var envelope = Assert.IsType<MessageEnvelope<TestEvent>>(pipeline.PublishedEnvelope);
        Assert.Equal(correlationId, envelope.Metadata.CorrelationId);
    }

    [Fact]
    public async Task PublishAsync_ForwardsCancellationToken()
    {
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, out var pipeline);
        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();
        using var cancellationTokenSource = new CancellationTokenSource();

        await publisher.PublishAsync(
            new TestEvent("value"),
            token: cancellationTokenSource.Token);

        Assert.Equal(cancellationTokenSource.Token, pipeline.PublishedCancellationToken);
    }

    [Fact]
    public async Task PublishAsync_WhenEventIsNull_Throws()
    {
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, out _);
        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => publisher.PublishAsync<TestEvent>(null!));
    }

    [Fact]
    public async Task PublishAsync_WhenCorrelationIdIsEmpty_ThrowsWithoutCallingPipeline()
    {
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, out var pipeline);
        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();

        await Assert.ThrowsAsync<ArgumentException>(
            () => publisher.PublishAsync(
                new TestEvent("value"),
                new MessageDispatchOptions { CorrelationId = Guid.Empty }));

        Assert.Equal(0, pipeline.PublishCalls);
    }

    private static ServiceProvider CreateProvider(DateTimeOffset utcNow, out CapturingOutboundMessagePipeline pipeline)
    {
        var services = new ServiceCollection();
        pipeline = new CapturingOutboundMessagePipeline();

        services.AddSingleton<TimeProvider>(new FixedTimeProvider(utcNow));
        services.AddSingleton<IOutboundMessagePipeline>(pipeline);
        services.AddAsyncomniaMessaging();

        return services.BuildServiceProvider();
    }
}
