using System.Reflection;
using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Handlers;
using Asyncomnia.Messaging.Inbound;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.DependencyInjection;

public static class HandlerScanningExtensions
{
    private static readonly MethodInfo CreateEventDescriptorMethod = typeof(MessageDispatchDescriptorFactory).GetMethod(nameof(MessageDispatchDescriptorFactory.CreateEvent))
        ?? throw new InvalidOperationException("MessageDispatchDescriptorFactory.CreateEvent method not found");

    private static readonly MethodInfo CreateCommandDescriptorMethod = typeof(MessageDispatchDescriptorFactory).GetMethod(nameof(MessageDispatchDescriptorFactory.CreateCommand))
        ?? throw new InvalidOperationException("MessageDispatchDescriptorFactory.CreateCommand method not found");

    private static readonly Type[] HandlerSupportedTypes = 
    [
        typeof(IEventHandler<>),
        typeof(ICommandHandler<>)
    ];

    public static AsyncomniaMessagingBuilder RegisterHandlersFromAssemblyContaining<TMarker>(this AsyncomniaMessagingBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.RegisterHandlersFromAssembly(typeof(TMarker).Assembly);
    }

    public static AsyncomniaMessagingBuilder RegisterHandlersFromAssembly(this AsyncomniaMessagingBuilder builder, Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(assembly);

        var handlerTypes = assembly.ExportedTypes.Where(IsConcreteHandler);

        foreach (var implementationType in handlerTypes)
        {
            var contracts = implementationType.GetInterfaces().Where(IsSupportedHandlerContract).Distinct();
            foreach (var contract in contracts)
            {
                builder.Services.TryAddEnumerable(ServiceDescriptor.Scoped(contract, implementationType));

                var messageType = contract.GenericTypeArguments[0];
                var handlerContractType = contract.GetGenericTypeDefinition();

                builder.Services.TryAddMessageHandlerType(messageType);

                var factoryMethod = handlerContractType == typeof(IEventHandler<>)
                    ? CreateEventDescriptorMethod
                    : CreateCommandDescriptorMethod;

                var descriptor = factoryMethod.MakeGenericMethod(messageType).Invoke(null, null);
                if (descriptor is not MessageDispatchDescriptor concreteDescriptor)
                {
                    throw new InvalidOperationException($"Could not create dispatch descriptor for message type {messageType}");
                }

                builder.Services.TryAddMessageDispatchDescriptor(concreteDescriptor);
            }
        }

        return builder;
    }

    private static bool IsConcreteHandler(Type type)
    {
        return type is
        {
            IsClass: true,
            IsAbstract: false,
            ContainsGenericParameters: false
        } && type.GetInterfaces().Any(IsSupportedHandlerContract);
    }

    private static bool IsSupportedHandlerContract(Type type)
    {
        if (!type.IsGenericType)
        {
            return false;
        }

        var genericDefinition = type.GetGenericTypeDefinition();
        return HandlerSupportedTypes.Contains(genericDefinition);
    }
}
