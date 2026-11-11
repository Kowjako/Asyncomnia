using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Reliability.DependencyInjection;

public sealed class InboxBuilder
{
    public IServiceCollection Services { get; }

    internal InboxBuilder(IServiceCollection services)
    {
        Services = services ?? throw new ArgumentNullException(nameof(services));
    }
}