using System.Reflection;
using EasyNetQ.Consumer;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal sealed class RabbitMqHandlerRegistrar(RabbitMqMessageConsumer messageConsumer)
{
    private static readonly MethodInfo RegisterEventMethod =
        typeof(RabbitMqHandlerRegistrar).GetMethod(nameof(RegisterEvent), BindingFlags.Instance | BindingFlags.NonPublic)!;

    private static readonly MethodInfo RegisterCommandMethod =
        typeof(RabbitMqHandlerRegistrar).GetMethod(nameof(RegisterCommand), BindingFlags.Instance | BindingFlags.NonPublic)!;

    public void Register(IHandlerRegistration registration, IReadOnlyCollection<RabbitMqBinding> bindings)
    {
        ArgumentNullException.ThrowIfNull(registration);
        ArgumentNullException.ThrowIfNull(bindings);

        registration.ThrowOnNoMatchingHandler = true;

        foreach (var messageType in bindings.Select(x => x.MessageType).Distinct())
        {
            var isEvent = typeof(IEvent).IsAssignableFrom(messageType);
            var isCommand = typeof(ICommand).IsAssignableFrom(messageType);

            if (isEvent == isCommand)
            {
                throw new InvalidOperationException($"Message type '{messageType.FullName}' must implement exactly one of {nameof(IEvent)} or {nameof(ICommand)}.");
            }

            var registrationMethod = isEvent ? RegisterEventMethod : RegisterCommandMethod;

            registrationMethod.MakeGenericMethod(messageType).Invoke(this, [registration]);
        }
    }

    private void RegisterEvent<TEvent>(IHandlerRegistration registration) where TEvent : class, IEvent
    {
        registration.Add<TEvent>((message, receivedInfo, ct) =>
            messageConsumer.ConsumeAsync(message, receivedInfo,
                static (pipeline, envelope, token) => pipeline.HandleEventAsync(envelope, token), ct));
    }

    private void RegisterCommand<TCommand>(IHandlerRegistration registration) where TCommand : class, ICommand
    {
        registration.Add<TCommand>((message, receivedInfo, ct) =>
            messageConsumer.ConsumeAsync(message, receivedInfo,
                static (pipeline, envelope, token) => pipeline.HandleCommandAsync(envelope, token), ct));
    }
}