namespace Asyncomnia.Messaging.Outbound;

internal sealed class CommandSender(MessageEnvelopeFactory factory, IOutboundMessagePipeline pipe) : ICommandSender
{
    public Task SendAsync<TCommand>(TCommand command, MessageDispatchOptions? options = null, CancellationToken token = default)
        where TCommand : class, ICommand
    {
        var envelope = factory.Create(command, options);
        return pipe.SendAsync(envelope, token);
    }
}