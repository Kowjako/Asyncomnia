using Asyncomnia.Messaging.Handlers;

namespace Asyncomnia.Messaging.Tests.TestDoubles;

public sealed record ScannedEvent(string Value) : IEvent;

public sealed record ScannedCommand(string Value) : ICommand;

public sealed record FirstMultiScannedEvent(string Value) : IEvent;

public sealed record SecondMultiScannedEvent(string Value) : IEvent;

public sealed class ScannedEventHandler : IEventHandler<ScannedEvent>
{
    public Task HandleAsync(ScannedEvent message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}

public sealed class ScannedCommandHandler : ICommandHandler<ScannedCommand>
{
    public Task HandleAsync(ScannedCommand message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}

public sealed class MultiScannedEventHandler : IEventHandler<FirstMultiScannedEvent>, IEventHandler<SecondMultiScannedEvent>
{
    public Task HandleAsync(FirstMultiScannedEvent message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task HandleAsync(SecondMultiScannedEvent message, MessageMetadata metadata, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}

public sealed class UnrelatedScannedType;
