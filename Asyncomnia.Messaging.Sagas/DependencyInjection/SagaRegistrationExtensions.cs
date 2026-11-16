using System.Reflection;
using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.Inbound;
using Asyncomnia.Messaging.Sagas.Configuration;
using Asyncomnia.Messaging.Sagas.Runtime;
using Asyncomnia.Messaging.Sagas.StateMachine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

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
        var messageTypes = stateMachine.MessageTypes;

        var conflictingMessageType = messageTypes.FirstOrDefault(t =>
            typeof(ICommand).IsAssignableFrom(t) && typeof(IEvent).IsAssignableFrom(t));

        if (conflictingMessageType is not null)
        {
            throw new InvalidOperationException($"Saga state machine '{typeof(TStateMachine).FullName}' registers message type '{conflictingMessageType.FullName}' as both an event and a command. A saga message must implement exactly one of these interfaces.");
        }

        var unsupportedMessageType = messageTypes.FirstOrDefault(t =>
            !typeof(ICommand).IsAssignableFrom(t) && !typeof(IEvent).IsAssignableFrom(t));

        if (unsupportedMessageType is not null)
        {
            throw new InvalidOperationException($"Saga state machine '{typeof(TStateMachine).FullName}' registers message type '{unsupportedMessageType.FullName}', which implements neither IEvent nor ICommand.");
        }

        // StateMachine should be singletone
        builder.Services.AddSingleton(stateMachine);

        // Runtime should be scoped, its dependent on sagaRepository
        // also DSL inside can invoke repositories (outbox, delayed) or resolve services from IServiceProvider
        builder.Services.AddScoped<SagaRuntime<TStateMachine, TSagaInstance>>();

        // Register SagaCommand/EventHandler for each type of command that saga is listening
        RegisterSagaHandlers<TStateMachine, TSagaInstance>(builder.Services, messageTypes);

        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<SagaOptions>, SagaOptionsValidator>());
        builder.Services.AddOptions<SagaOptions>().ValidateOnStart();

        return builder;
    }

    private static void RegisterSagaHandlers<TStateMachine, TSagaInstance>(IServiceCollection services, IReadOnlyList<Type> supportedSagaMessageTypes)
    {
        foreach (var messageType in supportedSagaMessageTypes)
        {
            var isEvent = typeof(IEvent).IsAssignableFrom(messageType);

            var handlerContractDefinition = isEvent
                ? typeof(IEventHandler<>)
                : typeof(ICommandHandler<>);

            var sagaHandlerDefinition = isEvent
                ? typeof(SagaEventHandler<,,>)
                : typeof(SagaCommandHandler<,,>);

            var descriptorFactory = isEvent ? CreateEventDescriptor : CreateCommandDescriptor;

            var messageKind = isEvent ? "event" : "command";

            var handlerContract = handlerContractDefinition.MakeGenericType(messageType);
            var sagaHandler = sagaHandlerDefinition.MakeGenericType(typeof(TStateMachine), typeof(TSagaInstance), messageType);

            services.TryAddEnumerable(ServiceDescriptor.Scoped(handlerContract, sagaHandler));
            services.TryAddMessageHandlerType(messageType);

            var descriptor = descriptorFactory.MakeGenericMethod(messageType).Invoke(null, null);

            if (descriptor is not MessageDispatchDescriptor typedDescriptor)
            {
                throw new InvalidOperationException($"Could not create a {messageKind} dispatch descriptor for message type '{messageType.FullName}' registered by saga state machine '{typeof(TStateMachine).FullName}'.");
            }

            services.TryAddMessageDispatchDescriptor(typedDescriptor);
        }
    }
}