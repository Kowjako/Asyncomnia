using System;
using System.Collections.Generic;
using System.Text;

namespace Asyncomnia.Messaging.Reliability.Outbox;

public interface IOutboxWriter
{
    Task WriteAsync(OutboxMessage message, CancellationToken token = default);
}