using Asyncomnia.Messaging.Internal.Serialization;
using Asyncomnia.Messaging.Reliability.Outbox;
using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Asyncomnia.Messaging.Reliability.Outbox.Processing;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Asyncomnia.Messaging.Reliability.Tests.TestDoubles;
using Asyncomnia.Messaging.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Asyncomnia.Messaging.Reliability.Tests.Outbox.Processing;

public sealed class OutboxBackgroundJobTests
{
    [Fact]
    public async Task StartAsync_WhenProcessorCannotBeResolved_PropagatesConfigurationError()
    {
        var options = new OutboxOptions();
        await using var provider = new ServiceCollection().BuildServiceProvider();
        using var job = CreateJob(provider, options, TimeProvider.System, out _);

        var exception = await Record.ExceptionAsync(async () =>
        {
            await job.StartAsync(CancellationToken.None);
            await (job.ExecuteTask ?? Task.CompletedTask);
        });

        var configurationException = Assert.IsType<InvalidOperationException>(exception);
        Assert.Contains(nameof(OutboxBatchProcessor), configurationException.Message);
    }

    [Fact]
    public async Task StartAsync_StartsConfiguredNumberOfWorkers()
    {
        var options = new OutboxOptions { WorkerCount = 3 };
        var workersEntered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var blocker = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var enteredWorkers = 0;
        var store = new ControlledOutboxStore(async (_, _, token) =>
        {
            if (Interlocked.Increment(ref enteredWorkers) == options.WorkerCount)
            {
                workersEntered.TrySetResult(true);
            }

            await blocker.Task.WaitAsync(token);
            return Array.Empty<ClaimedOutboxMessage>();
        });
        var transport = new Mock<IOutboundTransport>(MockBehavior.Strict);
        await using var provider = CreateProvider(() => CreateProcessor(store, transport.Object, options));
        using var job = CreateJob(provider, options, TimeProvider.System, out _);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await job.StartAsync(CancellationToken.None);
        await workersEntered.Task.WaitAsync(timeout.Token);

        Assert.Equal(options.WorkerCount, store.ClaimCalls);
        await job.StopAsync(timeout.Token);
    }

    [Fact]
    public async Task RunWorkerAsync_CreatesNewScopeForEachBatch()
    {
        var options = new OutboxOptions();
        var claimedMessage = CreateClaimedMessage();
        var secondBatchEntered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var blocker = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var claimCalls = 0;
        var processorInstances = 0;
        var store = new ControlledOutboxStore(async (_, _, token) =>
        {
            if (Interlocked.Increment(ref claimCalls) == 1)
            {
                return [claimedMessage];
            }

            secondBatchEntered.TrySetResult(true);
            await blocker.Task.WaitAsync(token);
            return Array.Empty<ClaimedOutboxMessage>();
        });
        var transport = new Mock<IOutboundTransport>(MockBehavior.Strict);
        transport
            .Setup(x => x.DispatchAsync(It.IsAny<IMessage>(), It.IsAny<MessageMetadata>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        await using var provider = CreateProvider(() =>
        {
            Interlocked.Increment(ref processorInstances);
            return CreateProcessor(store, transport.Object, options);
        });
        using var job = CreateJob(provider, options, TimeProvider.System, out _);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await job.StartAsync(CancellationToken.None);
        await secondBatchEntered.Task.WaitAsync(timeout.Token);

        Assert.Equal(2, Volatile.Read(ref processorInstances));
        await job.StopAsync(timeout.Token);
    }

    [Fact]
    public async Task RunWorkerAsync_WhenBatchIsEmpty_WaitsBeforePollingAgain()
    {
        var options = new OutboxOptions();
        var firstBatchProcessed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondBatchProcessed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new ControlledOutboxStore((_, _, _) =>
        {
            if (firstBatchProcessed.TrySetResult(true))
            {
                return Task.FromResult<IReadOnlyList<ClaimedOutboxMessage>>(
                    Array.Empty<ClaimedOutboxMessage>());
            }

            secondBatchProcessed.TrySetResult(true);
            return Task.FromResult<IReadOnlyList<ClaimedOutboxMessage>>(
                Array.Empty<ClaimedOutboxMessage>());
        });
        var transport = new Mock<IOutboundTransport>(MockBehavior.Strict);
        var timeProvider = new ManualTimeProvider();
        await using var provider = CreateProvider(() => CreateProcessor(store, transport.Object, options));
        using var job = CreateJob(provider, options, timeProvider, out _);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await job.StartAsync(CancellationToken.None);
        await firstBatchProcessed.Task.WaitAsync(timeout.Token);
        await timeProvider.WaitForTimerAsync(timeout.Token);

        Assert.Equal(1, store.ClaimCalls);
        timeProvider.FireNextTimer();
        await secondBatchProcessed.Task.WaitAsync(timeout.Token);

        Assert.Equal(2, store.ClaimCalls);
        await job.StopAsync(timeout.Token);
    }

    [Fact]
    public async Task RunWorkerAsync_WhenProcessingFails_LogsErrorAndWaitsBeforeRetrying()
    {
        var options = new OutboxOptions();
        var secondBatchEntered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var claimCalls = 0;
        var store = new ControlledOutboxStore((_, _, _) =>
        {
            if (Interlocked.Increment(ref claimCalls) == 1)
            {
                throw new InvalidOperationException("Database unavailable");
            }

            secondBatchEntered.TrySetResult(true);
            return Task.FromResult<IReadOnlyList<ClaimedOutboxMessage>>(
                Array.Empty<ClaimedOutboxMessage>());
        });
        var transport = new Mock<IOutboundTransport>(MockBehavior.Strict);
        var timeProvider = new ManualTimeProvider();
        await using var provider = CreateProvider(() => CreateProcessor(store, transport.Object, options));
        using var job = CreateJob(provider, options, timeProvider, out var logger);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await job.StartAsync(CancellationToken.None);
        await timeProvider.WaitForTimerAsync(timeout.Token);

        var error = Assert.Single(logger.Entries, entry => entry.LogLevel == LogLevel.Error);
        Assert.IsType<InvalidOperationException>(error.Exception);
        Assert.Equal(1, store.ClaimCalls);

        timeProvider.FireNextTimer();
        await secondBatchEntered.Task.WaitAsync(timeout.Token);
        await job.StopAsync(timeout.Token);
    }

    private static ServiceProvider CreateProvider(Func<OutboxBatchProcessor> processorFactory)
    {
        var services = new ServiceCollection();
        services.AddScoped(_ => processorFactory());

        return services.BuildServiceProvider();
    }

    private static OutboxBackgroundJob CreateJob(ServiceProvider provider, OutboxOptions options, TimeProvider timeProvider, out CapturingLogger<OutboxBackgroundJob> logger)
    {
        logger = new CapturingLogger<OutboxBackgroundJob>();

        return new OutboxBackgroundJob(
            Options.Create(options),
            timeProvider,
            logger,
            provider.GetRequiredService<IServiceScopeFactory>());
    }

    private static OutboxBatchProcessor CreateProcessor(IOutboxStore store, IOutboundTransport transport, OutboxOptions options)
    {
        return new OutboxBatchProcessor(
            store,
            transport,
            new MessageJsonSerializer(),
            Options.Create(options),
            NullLogger<OutboxBatchProcessor>.Instance);
    }

    private static ClaimedOutboxMessage CreateClaimedMessage()
    {
        var messageType = typeof(TestOutboxEvent);

        var outboxMessage = new OutboxMessage(
            Guid.NewGuid(),
            $"{messageType.FullName}, {messageType.Assembly.GetName().Name}",
            "{\"value\":\"event-value\"}",
            null,
            DateTimeOffset.UtcNow);

        return new ClaimedOutboxMessage(outboxMessage, Guid.NewGuid(), 1, DateTimeOffset.UtcNow.AddMinutes(1));
    }
}
