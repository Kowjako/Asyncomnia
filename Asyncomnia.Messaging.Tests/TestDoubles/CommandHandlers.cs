using Asyncomnia.Messaging.Handlers;

namespace Asyncomnia.Messaging.Tests.TestDoubles;

internal sealed class FirstCommandHandler(HandlerProbe probe) : ICommandHandler<TestCommand>
{
    public Task HandleAsync(TestCommand message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        probe.Calls.Add(nameof(FirstCommandHandler));
        probe.Metadata.Add(metadata);

        return Task.CompletedTask;
    }
}

internal sealed class SecondCommandHandler(HandlerProbe probe) : ICommandHandler<TestCommand>
{
    public Task HandleAsync(TestCommand message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        probe.Calls.Add(nameof(SecondCommandHandler));
        probe.Metadata.Add(metadata);

        return Task.CompletedTask;
    }
}
