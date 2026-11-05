using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Outbound;
using Asyncomnia.Messaging.Tests.TestDoubles;
using Asyncomnia.Messaging.Transport;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Tests.Transport;

public sealed class TransportMessagePipelineTests
{
    [Fact]
    public async Task PublishAsync_ForwardsEventMetadataAndCancellationTokenToTransport()
    {
        await using var provider = CreateProvider(out var transport);
        await using var scope = provider.CreateAsyncScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<IOutboundMessagePipeline>();
        var @event = new TestEvent("value");
        var envelope = TestEnvelopeFactory.Create(@event);
        using var cancellationTokenSource = new CancellationTokenSource();

        await pipeline.PublishAsync(envelope, cancellationTokenSource.Token);

        Assert.Same(@event, transport.Message);
        Assert.Same(envelope.Metadata, transport.Metadata);
        Assert.Equal(cancellationTokenSource.Token, transport.CancellationToken);
        Assert.Equal(1, transport.DispatchCalls);
    }

    [Fact]
    public async Task SendAsync_ForwardsCommandMetadataAndCancellationTokenToTransport()
    {
        await using var provider = CreateProvider(out var transport);
        await using var scope = provider.CreateAsyncScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<IOutboundMessagePipeline>();
        var command = new TestCommand("value");
        var envelope = TestEnvelopeFactory.Create(command);
        using var cancellationTokenSource = new CancellationTokenSource();

        await pipeline.SendAsync(envelope, cancellationTokenSource.Token);

        Assert.Same(command, transport.Message);
        Assert.Same(envelope.Metadata, transport.Metadata);
        Assert.Equal(cancellationTokenSource.Token, transport.CancellationToken);
        Assert.Equal(1, transport.DispatchCalls);
    }

    private static ServiceProvider CreateProvider(out CapturingOutboundMessageTransport transport)
    {
        var services = new ServiceCollection();
        transport = new CapturingOutboundMessageTransport();

        services.AddSingleton<IOutboundMessageTransport>(transport);
        services.AddAsyncomniaMessaging();

        return services.BuildServiceProvider();
    }
}
