using Asyncomnia.Messaging.Retry;
using Asyncomnia.Messaging.Tests.TestDoubles;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Tests.Retry;

public sealed class InMemoryRetryExecutorTests
{
    [Fact]
    public async Task ExecuteAsync_WithoutConfiguredRetries_ExecutesOperationOnce()
    {
        var attempts = 0;
        var executor = CreateExecutor(new InMemoryRetryOptions());

        await executor.ExecuteAsync(
            CreateEnvelope(),
            typeof(TestHandler),
            _ =>
            {
                attempts++;
                return Task.CompletedTask;
            });

        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task ExecuteAsync_WithImmediateRetries_RetriesUntilOperationSucceeds()
    {
        var options = new InMemoryRetryOptions();
        options.Immediately(2);
        var attempts = 0;
        var logger = new CapturingLogger<InMemoryRetryExecutor>();
        var executor = CreateExecutor(options, logger: logger);

        await executor.ExecuteAsync(
            CreateEnvelope(),
            typeof(TestHandler),
            _ =>
            {
                attempts++;

                return attempts < 3
                    ? Task.FromException(
                        new InvalidOperationException("transient failure"))
                    : Task.CompletedTask;
            });

        Assert.Equal(3, attempts);
        Assert.Equal(2, logger.Entries.Count);
        Assert.All(
            logger.Entries,
            entry => Assert.Equal(LogLevel.Warning, entry.LogLevel));
        Assert.Contains(
            logger.Entries,
            entry => entry.Message.Contains("1/2", StringComparison.Ordinal));
        Assert.Contains(
            logger.Entries,
            entry => entry.Message.Contains("2/2", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ExecuteAsync_WithIntervals_WaitsConfiguredDelayBeforeEachRetry()
    {
        var firstDelay = TimeSpan.FromSeconds(1);
        var secondDelay = TimeSpan.FromSeconds(2);
        var options = new InMemoryRetryOptions();
        options.Intervals(firstDelay, secondDelay);
        var timeProvider = new ManualTimeProvider();
        var attempts = 0;
        var executor = CreateExecutor(options, timeProvider);

        var execution = executor.ExecuteAsync(
            CreateEnvelope(),
            typeof(TestHandler),
            _ =>
            {
                attempts++;

                return attempts < 3
                    ? Task.FromException(
                        new InvalidOperationException("transient failure"))
                    : Task.CompletedTask;
            });

        await timeProvider.WaitForTimerAsync();
        Assert.Equal(1, attempts);
        Assert.Equal([firstDelay], timeProvider.DueTimes);
        timeProvider.FireNextTimer();

        await timeProvider.WaitForTimerAsync();
        Assert.Equal(2, attempts);
        Assert.Equal([firstDelay, secondDelay], timeProvider.DueTimes);
        timeProvider.FireNextTimer();

        await execution;
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task ExecuteAsync_WhenRetriesAreExhausted_PropagatesLastException()
    {
        var options = new InMemoryRetryOptions();
        options.Immediately(2);
        var attempts = 0;
        var firstException = new InvalidOperationException("first failure");
        var secondException = new InvalidOperationException("second failure");
        var lastException = new InvalidOperationException("last failure");
        var executor = CreateExecutor(options);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => executor.ExecuteAsync(
                CreateEnvelope(),
                typeof(TestHandler),
                _ =>
                {
                    attempts++;

                    return Task.FromException(
                        attempts switch
                        {
                            1 => firstException,
                            2 => secondException,
                            _ => lastException
                        });
                }));

        Assert.Same(lastException, exception);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task ExecuteAsync_WhenTokenIsAlreadyCancelled_DoesNotExecuteOperation()
    {
        var options = new InMemoryRetryOptions();
        options.Immediately(2);
        var attempts = 0;
        var executor = CreateExecutor(options);
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => executor.ExecuteAsync(
                CreateEnvelope(),
                typeof(TestHandler),
                _ =>
                {
                    attempts++;
                    return Task.CompletedTask;
                },
                cancellationTokenSource.Token));

        Assert.Equal(0, attempts);
    }

    [Fact]
    public async Task ExecuteAsync_WhenCancelledDuringDelay_DoesNotStartNextAttempt()
    {
        var options = new InMemoryRetryOptions();
        options.Intervals(TimeSpan.FromMinutes(1));
        var timeProvider = new ManualTimeProvider();
        var attempts = 0;
        var executor = CreateExecutor(options, timeProvider);
        using var cancellationTokenSource = new CancellationTokenSource();

        var execution = executor.ExecuteAsync(
            CreateEnvelope(),
            typeof(TestHandler),
            _ =>
            {
                attempts++;
                return Task.FromException(
                    new InvalidOperationException("transient failure"));
            },
            cancellationTokenSource.Token);

        await timeProvider.WaitForTimerAsync();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => execution);
        Assert.Equal(1, attempts);
    }

    private static InMemoryRetryExecutor CreateExecutor(
        InMemoryRetryOptions options,
        TimeProvider? timeProvider = null,
        CapturingLogger<InMemoryRetryExecutor>? logger = null)
    {
        return new InMemoryRetryExecutor(
            Options.Create(options),
            timeProvider ?? TimeProvider.System,
            logger);
    }

    private static MessageEnvelope<TestEvent> CreateEnvelope()
    {
        return TestEnvelopeFactory.Create(new TestEvent("value"));
    }

    private sealed class TestHandler
    {
    }
}
