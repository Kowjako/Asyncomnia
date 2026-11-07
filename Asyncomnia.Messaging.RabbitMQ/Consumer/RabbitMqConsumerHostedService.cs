using Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal sealed class RabbitMqConsumerHostedService(RabbitMqTopologyCreator topologyCreator, IRabbitMqQueueConsumerFactory consumerFactory) : IHostedService
{
    private readonly List<IAsyncDisposable> _consumers = [];

    public async Task StartAsync(CancellationToken ct)
    {
        var subscriptions = await topologyCreator.DeclareAsync(ct);

        try
        {
            foreach (var subscription in subscriptions)
            {
                var consumer = await consumerFactory.StartAsync(subscription, ct);
                _consumers.Add(consumer);
            }
        }
        catch
        {
            await DisposeConsumersAsync();
            throw;
        }
    }

    public async Task StopAsync(CancellationToken ct)
    {
        await DisposeConsumersAsync();
    }

    private async Task DisposeConsumersAsync()
    {
        for (var index = _consumers.Count - 1; index >= 0; index--)
        {
            await _consumers[index].DisposeAsync();
        }

        _consumers.Clear();
    }
}