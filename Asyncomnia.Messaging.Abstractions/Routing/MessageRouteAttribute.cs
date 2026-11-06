namespace Asyncomnia.Messaging.Abstractions.Routing;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class MessageRouteAttribute : Attribute
{
    public string Group { get; }

    public string? RoutingKey { get; }

    public MessageRouteAttribute(string group)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(group);
        Group = group;
    }

    public MessageRouteAttribute(string group, string routingKey) : this(group)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        RoutingKey = routingKey;
    }
}