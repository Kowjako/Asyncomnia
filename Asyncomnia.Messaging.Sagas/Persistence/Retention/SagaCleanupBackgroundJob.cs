using Asyncomnia.Messaging.Sagas.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Sagas.Persistence.Retention;

internal sealed class SagaCleanupBackgroundJob(IOptions<SagaOptions> options, TimeProvider timeProvider,
    IServiceScopeFactory scopeFactory, ILogger<SagaCleanupBackgroundJob> logger) : BackgroundService
{
    private readonly SagaOptions _options = options.Value;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.CleanupEnabled)
        {
            logger.LogInformation("Saga cleanup worker is disabled");
            return Task.CompletedTask;
        }

        return RunAsync(stoppingToken);
    }

    private async Task RunAsync(CancellationToken token)
    {
        logger.LogInformation("Saga cleanup worker started");

        while (!token.IsCancellationRequested)
        {
            var deletedCount = 0;

            try
            {
                await using var scope = scopeFactory.CreateAsyncScope();
                var processor = scope.ServiceProvider.GetRequiredService<SagaCleanupProcessor>();
                deletedCount = await processor.ProcessBatchAsync(token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Saga cleanup worker failed");
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
