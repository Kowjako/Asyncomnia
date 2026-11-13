using Asyncomnia.Messaging.Internal;
using Asyncomnia.Messaging.Internal.Serialization;
using Asyncomnia.Messaging.Outbound;

namespace Asyncomnia.Messaging.Reliability.DelayedPublishing;

internal sealed class MessageScheduler(
    MessageEnvelopeFactory envelopeFactory,
    MessageJsonSerializer serializer,
    IDelayedMessageWriter writer,
    TimeProvider timeProvider) : IMessageScheduler
{
    private readonly MessageEnvelopeFactory _envelopeFactory = envelopeFactory 
        ?? throw new ArgumentNullException(nameof(envelopeFactory));

    private readonly MessageJsonSerializer _serializer = serializer 
        ?? throw new ArgumentNullException(nameof(serializer));

    private readonly IDelayedMessageWriter _writer = writer 
        ?? throw new ArgumentNullException(nameof(writer));

    private readonly TimeProvider _timeProvider = timeProvider 
        ?? throw new ArgumentNullException(nameof(timeProvider));

    public Task<bool> CancelAsync(Guid scheduleId, CancellationToken token = default)
    {
        if (scheduleId == Guid.Empty)
        {
            throw new ArgumentException("Schedule ID cannot be empty", nameof(scheduleId));
        }

        return _writer.CancelAsync(scheduleId, token);
    }

    public async Task<Guid> ScheduleAsync<TMessage>(TMessage message, DateTimeOffset deliverAt, MessageDispatchOptions? options, CancellationToken token)
        where TMessage : class, IMessage
    {
        ArgumentNullException.ThrowIfNull(message);

        var envelope = _envelopeFactory.Create(message, options);
        var messageType = envelope.Message.GetType();
        var scheduleId = Guid.CreateVersion7();

        var delayedMessage = new DelayedMessage(scheduleId, envelope.Metadata.MessageId,
            ClrMessageTypeResolver.GetContractType(messageType), _serializer.SerializeToString(message),
            envelope.Metadata.CorrelationId, envelope.Metadata.CreatedAt, deliverAt);

        await _writer.WriteAsync(delayedMessage);
        return scheduleId;
    }

    public Task<Guid> ScheduleAsync<TMessage>(TMessage message, TimeSpan delay, MessageDispatchOptions? options, CancellationToken token)
        where TMessage: class, IMessage
    {
        if (delay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(delay), "Delay must be greater than zero");
        }

        var deliverAt = _timeProvider.GetUtcNow() + delay;
        return ScheduleAsync(message, deliverAt, options, token);
    }
}
