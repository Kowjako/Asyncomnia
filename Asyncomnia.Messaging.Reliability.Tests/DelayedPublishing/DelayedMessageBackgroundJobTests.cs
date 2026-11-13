using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.DelayedPublishing.Configuration;
using Asyncomnia.Messaging.Reliability.DelayedPublishing.Processing;
using Asyncomnia.Messaging.Reliability.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Reliability.Tests.DelayedPublishing;

public sealed class DelayedMessageBackgroundJobTests
{
    [Fact]
    public async Task RunAsync_WhenFullBatchIsMoved_ImmediatelyProcessesNextBatchInNewScope()
    {
        var options = new DelayedPublishingOptions { BatchSize = 10 };
        var secondBatchEntered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var blocker = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var processorInstances = 0;
        var store = new ControlledDelayedMessageStore(async (call, token) =>
        {
            if (call == 1)
            {
                return options.BatchSize;
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
    public async Task RunAsync_WhenPartialBatchIsMoved_WaitsBeforeNextBatch()
    {
        var options = new DelayedPublishingOptions { BatchSize = 10 };
        var firstBatchProcessed = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var secondBatchProcessed = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new ControlledDelayedMessageStore((call, _) =>
        {
            if (call == 1)
            {
                firstBatchProcessed.TrySetResult(true);
                return Task.FromResult(options.BatchSize - 1);
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
    public async Task RunAsync_WhenMovingFails_LogsErrorAndWaitsBeforeRetrying()
    {
        var options = new DelayedPublishingOptions();
        var secondBatchEntered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new ControlledDelayedMessageStore((call, _) =>
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
        IDelayedMessageStore store,
        DelayedPublishingOptions options,
        TimeProvider timeProvider,
        Action? processorCreated = null)
    {
        var services = new ServiceCollection();
        services.AddScoped(_ =>
        {
            processorCreated?.Invoke();

            return new DelayedMessageBatchProcessor(
                store,
                Options.Create(options),
                timeProvider);
        });

        return services.BuildServiceProvider();
    }

    private static DelayedMessageBackgroundJob CreateJob(
        ServiceProvider provider,
        DelayedPublishingOptions options,
        TimeProvider timeProvider,
        out CapturingLogger<DelayedMessageBackgroundJob> logger)
    {
        logger = new CapturingLogger<DelayedMessageBackgroundJob>();

        return new DelayedMessageBackgroundJob(
            Options.Create(options),
            timeProvider,
            provider.GetRequiredService<IServiceScopeFactory>(),
            logger);
    }

    private sealed class ControlledDelayedMessageStore(
        Func<int, CancellationToken, Task<int>> move) : IDelayedMessageStore
    {
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public Task<int> MoveDueToOutboxAsync(
            DateTimeOffset utcNow,
            int batchSize,
            CancellationToken token = default)
        {
            var call = Interlocked.Increment(ref _calls);
            return move(call, token);
        }
    }
}
