using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Outbox.Processing;

internal sealed class OutboxBackgroundJob : BackgroundService
{
    private readonly OutboxOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<OutboxBackgroundJob> _logger;
    private readonly IServiceScopeFactory _scopeFactory;

    public OutboxBackgroundJob(IOptions<OutboxOptions> options, TimeProvider timeProvider, ILogger<OutboxBackgroundJob> logger, 
        IServiceScopeFactory scopeFactory)
    {
        _options = options.Value;
        _timeProvider = timeProvider;
        _logger = logger;
        _scopeFactory = scopeFactory;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var workers = Enumerable.Range(0, _options.WorkerCount)
                                .Select(workerId => RunWorkerAsync(workerId, stoppingToken));

        return Task.WhenAll(workers);
    }

    private async Task RunWorkerAsync(int workerId, CancellationToken token)
    {
        _logger.LogInformation("Outbox worker[{workerId}] started", workerId);

        while (!token.IsCancellationRequested)
        {
            var processedCount = 0;

            await using var scope = _scopeFactory.CreateAsyncScope();
            var processor = scope.ServiceProvider.GetRequiredService<OutboxBatchProcessor>();

            try
            {
                processedCount = await processor.ProcessBatchAsync(token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Outbox worker[{workerId}] failed", workerId);
            }

            if (processedCount == 0)
            {
                try
                {
                    await Task.Delay(_options.PollingInterval, _timeProvider, token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }
}