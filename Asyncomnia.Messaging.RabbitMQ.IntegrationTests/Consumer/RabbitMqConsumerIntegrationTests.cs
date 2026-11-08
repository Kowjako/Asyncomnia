using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.DependencyInjection;
using Asyncomnia.Messaging.RabbitMQ.IntegrationTests.Infrastructure;
using Asyncomnia.Messaging.RabbitMQ.IntegrationTests.TestDoubles;
using EasyNetQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client.Exceptions;

namespace Asyncomnia.Messaging.RabbitMQ.IntegrationTests.Consumer;

[Collection(RabbitMqCollection.Name)]
public sealed class RabbitMqConsumerIntegrationTests
{
    private readonly string _connectionString;

    public RabbitMqConsumerIntegrationTests(RabbitMqContainerFixture fixture)
    {
        _connectionString = fixture.ConnectionString;
    }

    [Fact]
    public async Task Event_IsConsumedByRegisteredHandlerWithMetadata()
    {
        var endpointName = CreateEndpointName();
        var probe = new MessageProbe<ClientCreatedEvent>();
        using var host = CreateHost(
            endpointName,
            services => services.AddSingleton(probe),
            messaging => messaging
                .RegisterEventHandler<
                    ClientCreatedEvent,
                    ClientCreatedEventHandler>());
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var ct = cancellationTokenSource.Token;
        await host.StartAsync(ct);

        try
        {
            await using var scope = host.Services.CreateAsyncScope();
            var publisher = scope.ServiceProvider
                .GetRequiredService<IEventPublisher>();
            var @event = new ClientCreatedEvent(Guid.NewGuid(), "Acme");
            var correlationId = Guid.NewGuid();
            var earliestTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            await publisher.PublishAsync(
                @event,
                new MessageDispatchOptions { CorrelationId = correlationId },
                ct);
            var handled = await probe.WaitAsync(ct);
            var latestTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            Assert.Equal(@event, handled.Message);
            Assert.NotEqual(Guid.Empty, handled.Metadata.MessageId);
            Assert.Equal(correlationId, handled.Metadata.CorrelationId);
            Assert.InRange(
                handled.Metadata.CreatedAt.ToUnixTimeSeconds(),
                earliestTimestamp,
                latestTimestamp);
        }
        finally
        {
            await StopHostAsync(host);
        }
    }

    [Fact]
    public async Task EventAndCommand_AreConsumedFromIndependentQueues()
    {
        var endpointName = CreateEndpointName();
        var eventProbe = new MessageProbe<ClientCreatedEvent>();
        var commandProbe = new MessageProbe<StartLongWorkJobCommand>();
        using var host = CreateHost(
            endpointName,
            services =>
            {
                services.AddSingleton(eventProbe);
                services.AddSingleton(commandProbe);
            },
            messaging => messaging
                .RegisterEventHandler<
                    ClientCreatedEvent,
                    ClientCreatedEventHandler>()
                .RegisterCommandHandler<
                    StartLongWorkJobCommand,
                    StartLongWorkJobCommandHandler>());
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var ct = cancellationTokenSource.Token;
        await host.StartAsync(ct);

        try
        {
            await using var scope = host.Services.CreateAsyncScope();
            var publisher = scope.ServiceProvider
                .GetRequiredService<IEventPublisher>();
            var sender = scope.ServiceProvider
                .GetRequiredService<ICommandSender>();
            var @event = new ClientCreatedEvent(Guid.NewGuid(), "Acme");
            var command = new StartLongWorkJobCommand(Guid.NewGuid());

            await publisher.PublishAsync(@event, token: ct);
            await sender.SendAsync(command, token: ct);

            var handledEvent = await eventProbe.WaitAsync(ct);
            var handledCommand = await commandProbe.WaitAsync(ct);

            Assert.Equal(@event, handledEvent.Message);
            Assert.Equal(command, handledCommand.Message);

            var advancedBus = host.Services.GetRequiredService<IAdvancedBus>();
            var eventStats = await WaitForQueueStatsAsync(
                advancedBus,
                $"events/clients/subscription/{endpointName}",
                stats => stats.ConsumersCount == 1,
                ct);
            var commandStats = await WaitForQueueStatsAsync(
                advancedBus,
                $"commands/long-work-job/subscription/{endpointName}",
                stats => stats.ConsumersCount == 1,
                ct);

            Assert.Equal((ulong)1, eventStats.ConsumersCount);
            Assert.Equal((ulong)1, commandStats.ConsumersCount);
        }
        finally
        {
            await StopHostAsync(host);
        }
    }

    [Fact]
    public async Task ExactBindings_RouteOnlyHandledMessageTypesToQueue()
    {
        var endpointName = CreateEndpointName();
        var probe = new MessageProbe<ClientCreatedEvent>();

        using (var host = CreateHost(
                   endpointName,
                   services => services.AddSingleton(probe),
                   messaging => messaging
                       .RegisterEventHandler<
                           ClientCreatedEvent,
                           ClientCreatedEventHandler>()))
        {
            using var startupCancellation =
                new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await host.StartAsync(startupCancellation.Token);
            await StopHostAsync(host);
        }

        await using var provider = CreatePublisherProvider();
        var advancedBus = provider.GetRequiredService<IAdvancedBus>();
        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider
            .GetRequiredService<IEventPublisher>();
        var queueName = $"events/clients/subscription/{endpointName}";
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var ct = cancellationTokenSource.Token;

        await publisher.PublishAsync(
            new ClientContractSignedEvent(Guid.NewGuid(), "C-001"),
            token: ct);
        var afterUnhandled = await advancedBus.GetQueueStatsAsync(
            queueName,
            ct);

        Assert.Equal((ulong)0, afterUnhandled.MessagesCount);
        Assert.Equal((ulong)0, afterUnhandled.ConsumersCount);

        await publisher.PublishAsync(
            new ClientCreatedEvent(Guid.NewGuid(), "Acme"),
            token: ct);
        var afterHandled = await WaitForQueueStatsAsync(
            advancedBus,
            queueName,
            stats => stats.MessagesCount == 1,
            ct);

        Assert.Equal((ulong)1, afterHandled.MessagesCount);
    }

    [Fact]
    public async Task CompetingConsumers_ProcessTwoLongRunningCommandsConcurrently()
    {
        var endpointName = CreateEndpointName();
        var probe = new ConcurrentCommandProbe();
        using var host = CreateHost(
            endpointName,
            services => services.AddSingleton(probe),
            messaging => messaging
                .RegisterCommandHandler<
                    StartLongWorkJobCommand,
                    BlockingLongWorkJobCommandHandler>(),
            options => options.ConfigureSubscription(
                "commands/long-work-job",
                subscription =>
                {
                    subscription.ConsumerCount = 2;
                    subscription.PrefetchCount = 1;
                    subscription.DispatcherConcurrency = 1;
                }));
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var ct = cancellationTokenSource.Token;
        await host.StartAsync(ct);

        try
        {
            var advancedBus = host.Services.GetRequiredService<IAdvancedBus>();
            var queueStats = await WaitForQueueStatsAsync(
                advancedBus,
                $"commands/long-work-job/subscription/{endpointName}",
                stats => stats.ConsumersCount == 2,
                ct);
            Assert.Equal((ulong)2, queueStats.ConsumersCount);

            await using var scope = host.Services.CreateAsyncScope();
            var sender = scope.ServiceProvider
                .GetRequiredService<ICommandSender>();

            await sender.SendAsync(
                new StartLongWorkJobCommand(Guid.NewGuid()),
                token: ct);
            await sender.SendAsync(
                new StartLongWorkJobCommand(Guid.NewGuid()),
                token: ct);

            await probe.WaitUntilTwoStartedAsync(ct);

            Assert.Equal(2, probe.MaximumActive);

            probe.Release();
            await probe.WaitUntilAllCompletedAsync(ct);
        }
        finally
        {
            probe.Release();
            await StopHostAsync(host);
        }
    }

    [Fact]
    public async Task HandlerFailure_CreatesDeadLetterQueueAndPublishesMessage()
    {
        var endpointName = CreateEndpointName();
        using var host = CreateHost(
            endpointName,
            _ => { },
            messaging => messaging
                .RegisterEventHandler<
                    ClientCreatedEvent,
                    FailingClientCreatedEventHandler>());
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var ct = cancellationTokenSource.Token;
        await host.StartAsync(ct);

        try
        {
            await using var scope = host.Services.CreateAsyncScope();
            var publisher = scope.ServiceProvider
                .GetRequiredService<IEventPublisher>();

            await publisher.PublishAsync(
                new ClientCreatedEvent(Guid.NewGuid(), "Acme"),
                token: ct);

            var advancedBus = host.Services
                .GetRequiredService<IAdvancedBus>();
            var deadLetterQueueName =
                $"deadletter/events/clients/subscription/{endpointName}";
            var deadLetterStats = await WaitForQueueToBeCreatedAsync(
                advancedBus,
                deadLetterQueueName,
                stats => stats.MessagesCount == 1,
                ct);

            Assert.Equal((ulong)1, deadLetterStats.MessagesCount);
            Assert.Equal((ulong)0, deadLetterStats.ConsumersCount);
        }
        finally
        {
            await StopHostAsync(host);
        }
    }

    [Fact]
    public async Task TransientHandlerFailure_WithInMemoryRetries_SucceedsWithoutBrokerRedelivery()
    {
        var endpointName = CreateEndpointName();
        var probe = new InMemoryRetryProbe(failuresBeforeSuccess: 2);
        using var host = CreateHost(
            endpointName,
            services => services.AddSingleton(probe),
            messaging => messaging
                .RegisterEventHandler<
                    ClientCreatedEvent,
                    InMemoryRetryClientCreatedEventHandler>()
                .UseInMemoryRetries(options => options.Immediately(2)));
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var ct = cancellationTokenSource.Token;
        await host.StartAsync(ct);

        try
        {
            await using var scope = host.Services.CreateAsyncScope();
            var publisher = scope.ServiceProvider
                .GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(
                new ClientCreatedEvent(Guid.NewGuid(), "Acme"),
                token: ct);

            var attempts = await probe.WaitForSuccessAsync(ct);

            Assert.Equal(3, attempts);
            Assert.Equal(3, probe.Attempts);
        }
        finally
        {
            await StopHostAsync(host);
        }
    }

    [Fact]
    public async Task HandlerFailure_WithRepeatedBrokerRetryDelay_RetriesAndThenDeadLettersMessage()
    {
        const int expectedAttempts = 3;
        var endpointName = CreateEndpointName();
        var probe = new BrokerRetryProbe(expectedAttempts);
        using var host = CreateHost(
            endpointName,
            services => services.AddSingleton(probe),
            messaging => messaging
                .RegisterEventHandler<
                    ClientCreatedEvent,
                    BrokerRetryClientCreatedEventHandler>(),
            options =>
            {
                options.BrokerRetriesEnabled = true;
                options.BrokerRetryDelays =
                [
                    TimeSpan.FromMilliseconds(100),
                    TimeSpan.FromMilliseconds(100)
                ];
            });
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var ct = cancellationTokenSource.Token;
        await host.StartAsync(ct);

        try
        {
            var advancedBus = host.Services
                .GetRequiredService<IAdvancedBus>();
            var retryQueueName = "asyncomnia/retry-100ms-queue";
            var retryQueueStats = await advancedBus.GetQueueStatsAsync(
                retryQueueName,
                ct);
            Assert.Equal((ulong)0, retryQueueStats.MessagesCount);
            Assert.Equal((ulong)0, retryQueueStats.ConsumersCount);

            await using var scope = host.Services.CreateAsyncScope();
            var publisher = scope.ServiceProvider
                .GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(
                new ClientCreatedEvent(Guid.NewGuid(), "Acme"),
                token: ct);

            var attempts = await probe.WaitForExpectedAttemptsAsync(ct);
            var deadLetterQueueName =
                $"deadletter/events/clients/subscription/{endpointName}";
            var deadLetterStats = await WaitForQueueToBeCreatedAsync(
                advancedBus,
                deadLetterQueueName,
                stats => stats.MessagesCount == 1,
                ct);

            Assert.Equal(expectedAttempts, attempts);
            Assert.Equal(expectedAttempts, probe.Attempts);
            Assert.Equal((ulong)1, deadLetterStats.MessagesCount);

            retryQueueStats = await WaitForQueueStatsAsync(
                advancedBus,
                retryQueueName,
                stats => stats.MessagesCount == 0,
                ct);
            Assert.Equal((ulong)0, retryQueueStats.MessagesCount);
        }
        finally
        {
            await StopHostAsync(host);
        }
    }

    [Fact]
    public async Task HandlerFailure_WithInMemoryAndBrokerRetries_ExhaustsBothStrategiesBeforeDeadLettering()
    {
        const int expectedAttempts = 6;
        var endpointName = CreateEndpointName();
        var probe = new BrokerRetryProbe(expectedAttempts);
        using var host = CreateHost(
            endpointName,
            services => services.AddSingleton(probe),
            messaging => messaging
                .RegisterEventHandler<
                    ClientCreatedEvent,
                    BrokerRetryClientCreatedEventHandler>()
                .UseInMemoryRetries(options => options.Immediately(2)),
            options =>
            {
                options.BrokerRetriesEnabled = true;
                options.BrokerRetryDelays =
                [
                    TimeSpan.FromMilliseconds(100)
                ];
            });
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var ct = cancellationTokenSource.Token;
        await host.StartAsync(ct);

        try
        {
            await using var scope = host.Services.CreateAsyncScope();
            var publisher = scope.ServiceProvider
                .GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(
                new ClientCreatedEvent(Guid.NewGuid(), "Acme"),
                token: ct);

            var attempts = await probe.WaitForExpectedAttemptsAsync(ct);
            var advancedBus = host.Services
                .GetRequiredService<IAdvancedBus>();
            var deadLetterQueueName =
                $"deadletter/events/clients/subscription/{endpointName}";
            var deadLetterStats = await WaitForQueueToBeCreatedAsync(
                advancedBus,
                deadLetterQueueName,
                stats => stats.MessagesCount == 1,
                ct);

            Assert.Equal(expectedAttempts, attempts);
            Assert.Equal(expectedAttempts, probe.Attempts);
            Assert.Equal((ulong)1, deadLetterStats.MessagesCount);
        }
        finally
        {
            await StopHostAsync(host);
        }
    }

    private IHost CreateHost(string endpointName, Action<IServiceCollection> configureServices, Action<AsyncomniaMessagingBuilder> configureMessaging,
        Action<RabbitMqConsumerOptions>? configureConsumer = null)
    {
        var builder = Host.CreateApplicationBuilder();
        configureServices(builder.Services);

        var messaging = builder.Services.AddAsyncomniaMessaging();
        configureMessaging(messaging);
        messaging
            .UseRabbitMq(options => options.ConnectionString = _connectionString)
            .UseRabbitMqConsumer(options =>
            {
                options.EndpointName = endpointName;
                configureConsumer?.Invoke(options);
            });

        return builder.Build();
    }

    private ServiceProvider CreatePublisherProvider()
    {
        var services = new ServiceCollection();
        services
            .AddAsyncomniaMessaging()
            .UseRabbitMq(options => options.ConnectionString = _connectionString);

        return services.BuildServiceProvider();
    }

    private static async Task<QueueStats> WaitForQueueStatsAsync(IAdvancedBus advancedBus, string queueName,
        Func<QueueStats, bool> condition, CancellationToken ct)
    {
        while (true)
        {
            var stats = await advancedBus.GetQueueStatsAsync(queueName, ct);

            if (condition(stats))
            {
                return stats;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
        }
    }

    private static async Task<QueueStats> WaitForQueueToBeCreatedAsync(IAdvancedBus advancedBus, string queueName,
        Func<QueueStats, bool> condition, CancellationToken ct)
    {
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var stats = await advancedBus.GetQueueStatsAsync(
                    queueName,
                    ct);

                if (condition(stats))
                {
                    return stats;
                }
            }
            catch (OperationInterruptedException) when (!ct.IsCancellationRequested)
            {
                // The queue is created lazily after the handler fails.
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
        }
    }

    private static async Task StopHostAsync(IHost host)
    {
        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await host.StopAsync(cancellationTokenSource.Token);
    }

    private static string CreateEndpointName()
    {
        return $"integration-{Guid.NewGuid():N}";
    }
}
