using Asyncomnia.Messaging.Reliability.Inbox.Configuration;
using Asyncomnia.Messaging.Reliability.Inbox.Processing;
using Asyncomnia.Messaging.Reliability.Inbox.Storage;
using Asyncomnia.Messaging.Reliability.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Tests.Inbox;

public sealed class InboxCleanupBackgroundJobTests
{
    [Fact]
    public async Task StartAsync_WhenCleanupIsDisabled_DoesNotResolveProcessor()
    {
        var options = new InboxOptions { CleanupEnabled = false };
        await using var provider = new ServiceCollection().BuildServiceProvider();
        using var job = CreateJob(provider, options, TimeProvider.System, out var logger);

        await job.StartAsync(CancellationToken.None);
        await (job.ExecuteTask ?? Task.CompletedTask);

        Assert.Contains(
            logger.Entries,
            entry => entry.LogLevel == LogLevel.Information &&
                     entry.Message.Contains("disabled"));
    }

    [Fact]
    public async Task RunAsync_WhenFullBatchIsDeleted_ImmediatelyProcessesNextBatch()
    {
        var options = new InboxOptions { CleanupBatchSize = 10 };
        var secondBatchEntered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var blocker = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var processorInstances = 0;
        var store = new ControlledInboxCleanupStore(async (call, token) =>
        {
            if (call == 1)
            {
                return options.CleanupBatchSize;
            }

            secondBatchEntered.TrySetResult(true);
            await blocker.Task.WaitAsync(token);
            return 0;
        });
        await using var provider = CreateProvider(
            store,
            options,
            TimeProvider.System,
            () => Interlocked.Increment(ref processorInstances));
        using var job = CreateJob(provider, options, TimeProvider.System, out _);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await job.StartAsync(CancellationToken.None);
        await secondBatchEntered.Task.WaitAsync(timeout.Token);

        Assert.Equal(2, store.Calls);
        Assert.Equal(2, Volatile.Read(ref processorInstances));
        await job.StopAsync(timeout.Token);
    }

    [Fact]
    public async Task RunAsync_WhenPartialBatchIsDeleted_WaitsBeforeNextBatch()
    {
        var options = new InboxOptions { CleanupBatchSize = 10 };
        var firstBatchProcessed = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var secondBatchProcessed = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new ControlledInboxCleanupStore((call, _) =>
        {
            if (call == 1)
            {
                firstBatchProcessed.TrySetResult(true);
                return Task.FromResult(options.CleanupBatchSize - 1);
            }

            secondBatchProcessed.TrySetResult(true);
            return Task.FromResult(0);
        });
        var timeProvider = new ManualTimeProvider();
        await using var provider = CreateProvider(store, options, timeProvider);
        using var job = CreateJob(provider, options, timeProvider, out _);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await job.StartAsync(CancellationToken.None);
        await firstBatchProcessed.Task.WaitAsync(timeout.Token);
        await timeProvider.WaitForTimerAsync(timeout.Token);

        Assert.Equal(1, store.Calls);
        timeProvider.FireNextTimer();
        await secondBatchProcessed.Task.WaitAsync(timeout.Token);

        Assert.Equal(2, store.Calls);
        await job.StopAsync(timeout.Token);
    }

    [Fact]
    public async Task RunAsync_WhenCleanupFails_LogsErrorAndWaitsBeforeRetrying()
    {
        var options = new InboxOptions();
        var secondBatchEntered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new ControlledInboxCleanupStore((call, _) =>
        {
            if (call == 1)
            {
                throw new InvalidOperationException("Database unavailable");
            }

            secondBatchEntered.TrySetResult(true);
            return Task.FromResult(0);
        });
        var timeProvider = new ManualTimeProvider();
        await using var provider = CreateProvider(store, options, timeProvider);
        using var job = CreateJob(provider, options, timeProvider, out var logger);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await job.StartAsync(CancellationToken.None);
        await timeProvider.WaitForTimerAsync(timeout.Token);

        var error = Assert.Single(
            logger.Entries,
            entry => entry.LogLevel == LogLevel.Error);
        Assert.IsType<InvalidOperationException>(error.Exception);
        Assert.Equal(1, store.Calls);

        timeProvider.FireNextTimer();
        await secondBatchEntered.Task.WaitAsync(timeout.Token);
        await job.StopAsync(timeout.Token);
    }

    private static ServiceProvider CreateProvider(
        IInboxCleanupStore store,
        InboxOptions options,
        TimeProvider timeProvider,
        Action? processorCreated = null)
    {
        var services = new ServiceCollection();
        services.AddScoped(_ =>
        {
            processorCreated?.Invoke();

            return new InboxCleanupProcessor(
                store,
                Options.Create(options),
                timeProvider);
        });

        return services.BuildServiceProvider();
    }

    private static InboxCleanupBackgroundJob CreateJob(
        ServiceProvider provider,
        InboxOptions options,
        TimeProvider timeProvider,
        out CapturingLogger<InboxCleanupBackgroundJob> logger)
    {
        logger = new CapturingLogger<InboxCleanupBackgroundJob>();

        return new InboxCleanupBackgroundJob(
            Options.Create(options),
            timeProvider,
            provider.GetRequiredService<IServiceScopeFactory>(),
            logger);
    }

    private sealed class ControlledInboxCleanupStore(
        Func<int, CancellationToken, Task<int>> delete) : IInboxCleanupStore
    {
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public Task<int> DeleteExpiredAsync(
            DateTimeOffset expiresBefore,
            int batchSize,
            CancellationToken token = default)
        {
            var call = Interlocked.Increment(ref _calls);
            return delete(call, token);
        }
    }
}
