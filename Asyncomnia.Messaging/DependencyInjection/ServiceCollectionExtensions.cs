using System;
using System.Collections.Generic;
using System.Text;
using Asyncomnia.Messaging.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static AsyncomniaMessagingBuilder AddAsyncomniaMessaging(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services, nameof(services));

        return new AsyncomniaMessagingBuilder(services);
    }
}
