using System;
using System.Collections.Generic;
using System.Text;

namespace Asyncomnia.Messaging.Reliability.Internal;

internal static class ClrMessageTypeResolver
{
    public static string GetContractType(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        var typeName = messageType.FullName 
            ?? throw new InvalidOperationException($"Message type '{messageType}' does not have a full name");

        var assemblyName = messageType.Assembly.GetName().Name 
            ?? throw new InvalidOperationException($"Message type '{messageType}' does not have an assembly name");

        return $"{typeName}, {assemblyName}";
    }

    public static Type Resolve(string contractType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(contractType);

        return Type.GetType(contractType, throwOnError: false)
            ?? throw new InvalidOperationException($"Could not resolve messsage contract type '{contractType}'");
    }
}
