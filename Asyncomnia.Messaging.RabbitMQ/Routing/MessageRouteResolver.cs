using System.Collections.Concurrent;
using System.Reflection;
using System.Text.RegularExpressions;
using Asyncomnia.Messaging.Routing;

namespace Asyncomnia.Messaging.RabbitMQ.Routing;

internal sealed class MessageRouteResolver
{
    private readonly ConcurrentDictionary<Type, MessageRoute> _routes = new();

    public MessageRoute Resolve(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return _routes.GetOrAdd(messageType, CreateRoute);
    }

    private static MessageRoute CreateRoute(Type messageType)
    {
        var attribute = messageType.GetCustomAttribute<MessageRouteAttribute>()
            ?? throw new InvalidOperationException($"Message type '{messageType.FullName}' must define MessageRouteAttribute.");

        var isEvent = typeof(IEvent).IsAssignableFrom(messageType);
        var isCommand = typeof(ICommand).IsAssignableFrom(messageType);

        if (isEvent == isCommand)
        {
            throw new InvalidOperationException($"Message type '{messageType.FullName}' must implement exactly one of IEvent or ICommand.");
        }

        var exchangePrefix = isEvent ? "events" : "commands";
        var exchangeName = $"{exchangePrefix}/{attribute.Group}";

        var routingKey = attribute.RoutingKey ?? ToKebabCase(messageType.Name);

        return new MessageRoute(exchangeName, routingKey);
    }

    private static string ToKebabCase(string value)
    {
        var result = Regex.Replace(value, "([A-Z]+)([A-Z][a-z])", "$1-$2");
        result = Regex.Replace(result, "([a-z0-9])([A-Z])", "$1-$2");
        return result.Replace('_', '-').ToLowerInvariant();
    }
}