using Asyncomnia.Messaging.Reliability.Inbox.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Reliability.DependencyInjection;

public sealed class InboxBuilder
{
    public IServiceCollection Services { get; }

    internal InboxBuilder(IServiceCollection services)
    {
        Services = services ?? throw new ArgumentNullException(nameof(services));
    }

    public InboxBuilder Configure(Action<InboxOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        Services.AddOptions<InboxOptions>().Configure(configure);
        return this;
    }
}