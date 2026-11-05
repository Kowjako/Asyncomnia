using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Outbound;
using Asyncomnia.Messaging.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Tests.Outbound;

public sealed class CommandSenderTests
{
    [Fact]
    public async Task SendAsync_CreatesEnvelopeAndUsesSendPipeline()
    {
        var utcNow = new DateTimeOffset(2026, 11, 5, 12, 0, 0, TimeSpan.Zero);
        await using var provider = CreateProvider(utcNow, out var pipeline);
        await using var scope = provider.CreateAsyncScope();
        var sender = scope.ServiceProvider.GetRequiredService<ICommandSender>();
        var command = new TestCommand("value");

        await sender.SendAsync(command);

        var envelope = Assert.IsType<MessageEnvelope<TestCommand>>(pipeline.SentEnvelope);
        Assert.Same(command, envelope.Message);
        Assert.NotEqual(Guid.Empty, envelope.Metadata.MessageId);
        Assert.Equal(utcNow, envelope.Metadata.CreatedAt);
        Assert.Null(envelope.Metadata.CorrelationId);
        Assert.Equal(1, pipeline.SendCalls);
        Assert.Equal(0, pipeline.PublishCalls);
    }

    [Fact]
    public async Task SendAsync_ForwardsCorrelationIdAndCancellationToken()
    {
        var correlationId = Guid.NewGuid();
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, out var pipeline);
        await using var scope = provider.CreateAsyncScope();
        var sender = scope.ServiceProvider.GetRequiredService<ICommandSender>();
        using var cancellationTokenSource = new CancellationTokenSource();

        await sender.SendAsync(
            new TestCommand("value"),
            new MessageDispatchOptions { CorrelationId = correlationId },
            cancellationTokenSource.Token);

        var envelope = Assert.IsType<MessageEnvelope<TestCommand>>(pipeline.SentEnvelope);
        Assert.Equal(correlationId, envelope.Metadata.CorrelationId);
        Assert.Equal(cancellationTokenSource.Token, pipeline.SentCancellationToken);
    }

    [Fact]
    public async Task SendAsync_WhenCommandIsNull_Throws()
    {
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, out _);
        await using var scope = provider.CreateAsyncScope();
        var sender = scope.ServiceProvider.GetRequiredService<ICommandSender>();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => sender.SendAsync<TestCommand>(null!));
    }

    private static ServiceProvider CreateProvider(
        DateTimeOffset utcNow,
        out CapturingOutboundMessagePipeline pipeline)
    {
        var services = new ServiceCollection();
        pipeline = new CapturingOutboundMessagePipeline();

        services.AddSingleton<TimeProvider>(new FixedTimeProvider(utcNow));
        services.AddSingleton<IOutboundMessagePipeline>(pipeline);
        services.AddAsyncomniaMessaging();

        return services.BuildServiceProvider();
    }
}
