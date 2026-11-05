namespace Asyncomnia.Messaging;

public sealed record MessageDispatchOptions
{
    public Guid? CorrelationId { get; init; }
}