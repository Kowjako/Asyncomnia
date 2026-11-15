using System.Reflection;
using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Sagas.Runtime;
using Asyncomnia.Messaging.Sagas.StateMachine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.Sagas.DependencyInjection;

public static class SagaRegistrationExtensions
{
    private static readonly MethodInfo CreateEventDescriptor = typeof(MessageDispatchDescriptorFactory)
        .GetMethod(nameof(MessageDispatchDescriptorFactory.CreateEvent)) ?? throw new InvalidOperationException($"Could not find method '{nameof(MessageDispatchDescriptorFactory.CreateEvent)}' on '{typeof(MessageDispatchDescriptorFactory).FullName}'.");

    private static readonly MethodInfo CreateCommandDescriptor = typeof(MessageDispatchDescriptorFactory)
        .GetMethod(nameof(MessageDispatchDescriptorFactory.CreateCommand)) ?? throw new InvalidOperationException($"Could not find method '{nameof(MessageDispatchDescriptorFactory.CreateCommand)}' on '{typeof(MessageDispatchDescriptorFactory).FullName}'.");

    public static AsyncomniaMessagingBuilder RegisterAsyncomniaSaga<TStateMachine, TSagaInstance>(this AsyncomniaMessagingBuilder builder)
        where TStateMachine : AsyncomniaStateMachine<TSagaInstance>, new()
        where TSagaInstance : AsyncomniaSagaInstance, new()
    {
        ArgumentNullException.ThrowIfNull(builder);

        var stateMachine = new TStateMachine();

        // StateMachine should be singletone
        builder.Services.AddSingleton(stateMachine);

        // Runtime should be scoped, its dependent on sagaRepository
        // also DSL inside can invoke repositories (outbox, delayed) or resolve services from IServiceProvider
        builder.Services.AddScoped<SagaRuntime<TStateMachine, TSagaInstance>>();

        var conflictingMessageType = stateMachine.MessageTypes.FirstOrDefault(t =>
            typeof(ICommand).IsAssignableFrom(t) && typeof(IEvent).IsAssignableFrom(t));

        if (conflictingMessageType is not null)
        {
            throw new InvalidOperationException($"Saga state machine '{typeof(TStateMachine).FullName}' registers message type '{conflictingMessageType.FullName}' as both an event and a command. A saga message must implement exactly one of these interfaces.");
        }

        var unsupportedMessageType = stateMachine.MessageTypes.FirstOrDefault(t =>
            !typeof(ICommand).IsAssignableFrom(t) && !typeof(IEvent).IsAssignableFrom(t));

        if (unsupportedMessageType is not null)
        {
            throw new InvalidOperationException($"Saga state machine '{typeof(TStateMachine).FullName}' registers message type '{unsupportedMessageType.FullName}', which implements neither IEvent nor ICommand.");
        }

        // Register SagaEventHandler for each type of events that saga listening
        foreach (var sagaEventType in stateMachine.MessageTypes)
        {
            if (!typeof(IEvent).IsAssignableFrom(sagaEventType))
            {
                continue;
            }

            var eventHandlerType = typeof(IEventHandler<>).MakeGenericType(sagaEventType);

            var sagaEventHandlerType = typeof(SagaEventHandler<,,>)
                .MakeGenericType(typeof(TStateMachine), typeof(TSagaInstance), sagaEventType);

            // register handler
            builder.Services.TryAddEnumerable(ServiceDescriptor.Scoped(eventHandlerType, sagaEventHandlerType));

            // add this event to catalog, so topology creator will take care of creating subscription
            builder.Services.TryAddMessageHandlerType(sagaEventType);

            // add dispatcher descriptor, so consumer will know how to run this event
            var methodInfo = CreateEventDescriptor.MakeGenericMethod(sagaEventType);
            var descriptor = methodInfo.Invoke(null, null);

            if (descriptor is not MessageDispatchDescriptor typedDescriptor)
            {
                throw new InvalidOperationException($"Could not create an event dispatch descriptor for message type '{sagaEventType.FullName}' registered by saga state machine '{typeof(TStateMachine).FullName}'.");
            }

            builder.Services.TryAddMessageDispatchDescriptor(typedDescriptor);
        }

        // Register SagaCommandHandler for each type of command that saga is listening
        foreach (var sagaEventType in stateMachine.MessageTypes)
        {
            if (!typeof(ICommand).IsAssignableFrom(sagaEventType))
            {
                continue;
            }

            var eventHandlerType = typeof(ICommandHandler<>).MakeGenericType(sagaEventType);

            var sagaEventHandlerType = typeof(SagaCommandHandler<,,>)
                .MakeGenericType(typeof(TStateMachine), typeof(TSagaInstance), sagaEventType);

            builder.Services.TryAddEnumerable(ServiceDescriptor.Scoped(eventHandlerType, sagaEventHandlerType));
            builder.Services.TryAddMessageHandlerType(sagaEventType);

            // add dispatcher descriptor, so consumer will know how to run this event
            var methodInfo = CreateCommandDescriptor.MakeGenericMethod(sagaEventType);
            var descriptor = methodInfo.Invoke(null, null);

            if (descriptor is not MessageDispatchDescriptor typedDescriptor)
            {
                throw new InvalidOperationException($"Could not create a command dispatch descriptor for message type '{sagaEventType.FullName}' registered by saga state machine '{typeof(TStateMachine).FullName}'.");
            }

            builder.Services.TryAddMessageDispatchDescriptor(typedDescriptor);
        }

        return builder;
    }
}