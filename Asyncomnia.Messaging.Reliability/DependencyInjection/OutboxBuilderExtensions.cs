using System;
using System.Collections.Generic;
using System.Text;
using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Outbound;
using Asyncomnia.Messaging.Reliability.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Asyncomnia.Messaging.Reliability.DependencyInjection;

public static class OutboxBuilderExtensions
{
    public static AsyncomniaMessagingBuilder UseOutbox(this AsyncomniaMessagingBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.TryAddSingleton<OutboxJsonSerializer>();

        builder.Services.RemoveAll<IOutboundMessagePipeline>();
        builder.Services.AddScoped<IOutboundMessagePipeline, OutboxMessagePipeline>();

        return builder;
    }
}
