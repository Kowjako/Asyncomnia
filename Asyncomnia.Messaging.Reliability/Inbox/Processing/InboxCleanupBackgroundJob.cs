using Asyncomnia.Messaging.Reliability.Inbox.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Inbox.Processing;

internal sealed class InboxCleanupBackgroundJob(
    IOptions<InboxOptions> options,
    TimeProvider timeProvider,
    IServiceScopeFactory scopeFactory,
    ILogger<InboxCleanupBackgroundJob> logger) : BackgroundService
{
    private readonly InboxOptions _options = options.Value;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.CleanupEnabled)
        {
            logger.LogInformation("Inbox cleanup worker is disabled");
            return Task.CompletedTask;
        }

        return RunAsync(stoppingToken);
    }

    private async Task RunAsync(CancellationToken token)
    {
        logger.LogInformation("Inbox cleanup worker started");

        while (!token.IsCancellationRequested)
        {
            var deletedCount = 0;

            try
            {
                await using var scope = scopeFactory.CreateAsyncScope();

                var processor = scope.ServiceProvider.GetRequiredService<InboxCleanupProcessor>();

                deletedCount = await processor.ProcessBatchAsync(token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Inbox cleanup worker failed");
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