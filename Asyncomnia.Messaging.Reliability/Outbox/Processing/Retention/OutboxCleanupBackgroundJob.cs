using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Outbox.Processing.Retention;

internal sealed class OutboxCleanupBackgroundJob(IOptions<OutboxOptions> options, TimeProvider timeProvider,
    IServiceScopeFactory scopeFactory, ILogger<OutboxCleanupBackgroundJob> logger) : BackgroundService
{
    private readonly OutboxOptions _options = options.Value;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.CleanupEnabled)
        {
            logger.LogInformation("Outbox cleanup worker is disabled");
            return Task.CompletedTask;
        }

        return RunAsync(stoppingToken);
    }

    private async Task RunAsync(CancellationToken token)
    {
        logger.LogInformation("Outbox cleanup worker started");

        while (!token.IsCancellationRequested)
        {
            var deletedCount = 0;

            try
            {
                await using var scope = scopeFactory.CreateAsyncScope();
                var processor = scope.ServiceProvider.GetRequiredService<OutboxCleanupProcessor>();
                deletedCount = await processor.ProcessBatchAsync(token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Outbox cleanup worker failed");
            }

            if (deletedCount < _options.CleanupBatchSize)
            {
                try
                {
                    await Task.Delay(_options.CleanupInterval, timeProvider, token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }
}