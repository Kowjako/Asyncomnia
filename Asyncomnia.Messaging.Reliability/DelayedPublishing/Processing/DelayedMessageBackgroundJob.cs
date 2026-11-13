using Asyncomnia.Messaging.Reliability.DelayedPublishing.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.DelayedPublishing.Processing;

internal sealed class DelayedMessageBackgroundJob(
    IOptions<DelayedPublishingOptions> options,
    TimeProvider timeProvider,
    IServiceScopeFactory scopeFactory,
    ILogger<DelayedMessageBackgroundJob> logger) : BackgroundService
{
    private readonly DelayedPublishingOptions _options = options.Value;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return RunAsync(stoppingToken);
    }

    private async Task RunAsync(CancellationToken token)
    {
        logger.LogInformation("Delayed message worker started");

        while (!token.IsCancellationRequested)
        {
            var movedCount = 0;

            try
            {
                await using var scope = scopeFactory.CreateAsyncScope();
                var processor = scope.ServiceProvider.GetRequiredService<DelayedMessageBatchProcessor>();
                movedCount = await processor.ProcessBatchAsync(token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Delayed message worker failed");
            }

            if (movedCount < _options.BatchSize)
            {
                try
                {
                    await Task.Delay(_options.PollingInterval, timeProvider, token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }
}