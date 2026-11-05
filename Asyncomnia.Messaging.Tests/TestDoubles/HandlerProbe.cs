using System;
using System.Collections.Generic;

namespace Asyncomnia.Messaging.Tests.TestDoubles;

internal sealed class HandlerProbe
{
    public List<string> Calls { get; } = [];

    public List<Guid> HandlerInstances { get; } = [];

    public List<MessageMetadata> Metadata { get; } = [];
}
