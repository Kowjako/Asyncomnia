using Asyncomnia.Messaging.Handlers;

namespace Asyncomnia.Messaging.Tests.TestDoubles;

internal sealed class FirstEventHandler(HandlerProbe probe) : IEventHandler<TestEvent>
{
    private readonly Guid instanceId = Guid.NewGuid();

    public Task HandleAsync(TestEvent message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        probe.Calls.Add(nameof(FirstEventHandler));
        probe.HandlerInstances.Add(instanceId);
        probe.Metadata.Add(metadata);

        return Task.CompletedTask;
    }
}

internal sealed class SecondEventHandler(HandlerProbe probe) : IEventHandler<TestEvent>
{
    private readonly Guid instanceId = Guid.NewGuid();

    public Task HandleAsync(TestEvent message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        probe.Calls.Add(nameof(SecondEventHandler));
        probe.HandlerInstances.Add(instanceId);
        probe.Metadata.Add(metadata);

        return Task.CompletedTask;
    }
}
