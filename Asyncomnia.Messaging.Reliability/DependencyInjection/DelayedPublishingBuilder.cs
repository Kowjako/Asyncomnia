using Asyncomnia.Messaging.Reliability.DelayedPublishing.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Reliability.DependencyInjection;

public sealed class DelayedPublishingBuilder
{
    public IServiceCollection Services { get; }

    internal DelayedPublishingBuilder(IServiceCollection services)
    {
        Services = services;
    }

    public DelayedPublishingBuilder Configure(Action<DelayedPublishingOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        Services.AddOptions<DelayedPublishingOptions>().Configure(configure);

        return this;
    }
}