using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Reliability.DependencyInjection;

public sealed class OutboxBuilder
{
    public IServiceCollection Services { get; }

    internal OutboxBuilder(IServiceCollection services)
    {
        Services = services;
    }

    public OutboxBuilder Configure(Action<OutboxOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure, nameof(configure));

        Services.AddOptions<OutboxOptions>().Configure(configure);
        return this;
    }
}
