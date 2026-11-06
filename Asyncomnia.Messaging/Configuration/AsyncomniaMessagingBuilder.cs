using Microsoft.Extensions.DependencyInjection;

namespace Asyncomnia.Messaging.Configuration;

public sealed class AsyncomniaMessagingBuilder
{
    public IServiceCollection Services { get; }

	public AsyncomniaMessagingBuilder(IServiceCollection services)
	{
		Services = services ?? throw new ArgumentNullException(nameof(services));
	}
}