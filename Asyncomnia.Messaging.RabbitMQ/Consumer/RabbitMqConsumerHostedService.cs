using Asyncomnia.Messaging.RabbitMQ.Consumer.DeadLettering;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Retry;
using Asyncomnia.Messaging.RabbitMQ.Consumer.Topology;
using Microsoft.Extensions.Hosting;

namespace Asyncomnia.Messaging.RabbitMQ.Consumer;

internal sealed class RabbitMqConsumerHostedService(
    RabbitMqTopologyCreator topologyCreator,
    IRabbitMqRetryTopologyCreator retryTopologyCreator,
    IRabbitMqDeadLetterTopologyCreator deadLetterTopologyCreator,
    IRabbitMqQueueConsumerFactory consumerFactory) : IHostedService
{
    private readonly List<IAsyncDisposable> _consumers = [];

    public async Task StartAsync(CancellationToken ct)
    {
        var subscriptions = await topologyCreator.DeclareAsync(ct);
        await deadLetterTopologyCreator.DeclareAsync(subscriptions, ct);
        await retryTopologyCreator.DeclareAsync(ct);

        try
        {
            foreach (var declaredSubscription in subscriptions)
            {
                var consumer = await consumerFactory.StartAsync(declaredSubscription, ct);
                _consumers.Add(consumer);
            }
        }
        catch (Exception startException)
        {
            try
            {
                await DisposeConsumersAsync();
            }
            catch (Exception disposeException)
            {
                throw new AggregateException("RabbitMQ consumers could not be started or cleaned up.", startException, disposeException);
            }

            throw;
        }
    }

    public async Task StopAsync(CancellationToken ct)
    {
        await DisposeConsumersAsync();
    }

    private async Task DisposeConsumersAsync()
    {
        List<Exception>? exceptions = null;

        for (var index = _consumers.Count - 1; index >= 0; index--)
        {
            try
            {
                await _consumers[index].DisposeAsync();
            }
            catch (Exception exception)
            {
                exceptions ??= [];
                exceptions.Add(exception);
            }
        }

        _consumers.Clear();

        if (exceptions is not null)
        {
            throw new AggregateException("One or more RabbitMQ consumers could not be disposed.", exceptions);
        }
    }
}