using System.Reflection;
using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.DependencyInjection;

public static class HandlerScanningExtensions
{
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
