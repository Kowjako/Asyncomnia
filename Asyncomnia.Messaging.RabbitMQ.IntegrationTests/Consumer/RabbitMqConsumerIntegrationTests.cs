using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.RabbitMQ.Configuration;
using Asyncomnia.Messaging.RabbitMQ.DependencyInjection;
using Asyncomnia.Messaging.RabbitMQ.IntegrationTests.Infrastructure;
using Asyncomnia.Messaging.RabbitMQ.IntegrationTests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;

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

            var eventStats = await WaitForQueueStatsAsync(
                $"events/clients/subscription/{endpointName}",
                stats => stats.ConsumerCount == 1,
                ct);
            var commandStats = await WaitForQueueStatsAsync(
                $"commands/long-work-job/subscription/{endpointName}",
                stats => stats.ConsumerCount == 1,
                ct);

            Assert.Equal((uint)1, eventStats.ConsumerCount);
            Assert.Equal((uint)1, commandStats.ConsumerCount);
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
        var afterUnhandled = await GetQueueStatsAsync(queueName, ct);

        Assert.Equal((uint)0, afterUnhandled.MessageCount);
        Assert.Equal((uint)0, afterUnhandled.ConsumerCount);

        await publisher.PublishAsync(
            new ClientCreatedEvent(Guid.NewGuid(), "Acme"),
            token: ct);
        var afterHandled = await WaitForQueueStatsAsync(
            queueName,
            stats => stats.MessageCount == 1,
            ct);

        Assert.Equal((uint)1, afterHandled.MessageCount);
    }

    [Fact]
    public async Task CompetingConsumers_AcrossTwoHosts_ProcessLongRunningCommandsConcurrently()
    {
        var endpointName = CreateEndpointName();
        var probe = new ConcurrentCommandProbe();
        using var firstHost = CreateHost(
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
                    subscription.PrefetchCount = 1;
                    subscription.DispatcherConcurrency = 1;
                }));
        using var secondHost = CreateHost(
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
                    subscription.PrefetchCount = 1;
                    subscription.DispatcherConcurrency = 1;
                }));
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var ct = cancellationTokenSource.Token;
        await firstHost.StartAsync(ct);
        await secondHost.StartAsync(ct);

        try
        {
            var queueStats = await WaitForQueueStatsAsync(
                $"commands/long-work-job/subscription/{endpointName}",
                stats => stats.ConsumerCount == 2,
                ct);
            Assert.Equal((uint)2, queueStats.ConsumerCount);

            await using var scope = firstHost.Services.CreateAsyncScope();
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
            await StopHostAsync(secondHost);
            await StopHostAsync(firstHost);
        }
    }

    [Fact]
    public async Task SingleActiveConsumer_AcrossTwoHosts_ProcessesCommandsSequentially()
    {
        var endpointName = CreateEndpointName();
        var probe = new ConcurrentCommandProbe();
        Action<RabbitMqConsumerOptions> configureConsumer = options =>
            options.ConfigureSubscription(
                "commands/long-work-job",
                subscription =>
                {
                    subscription.SingleActiveConsumer = true;
                    subscription.PrefetchCount = 1;
                    subscription.DispatcherConcurrency = 1;
                });
        using var firstHost = CreateHost(
            endpointName,
            services => services.AddSingleton(probe),
            messaging => messaging
                .RegisterCommandHandler<
                    StartLongWorkJobCommand,
                    BlockingLongWorkJobCommandHandler>(),
            configureConsumer);
        using var secondHost = CreateHost(
            endpointName,
            services => services.AddSingleton(probe),
            messaging => messaging
                .RegisterCommandHandler<
                    StartLongWorkJobCommand,
                    BlockingLongWorkJobCommandHandler>(),
            configureConsumer);
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var ct = cancellationTokenSource.Token;
        await firstHost.StartAsync(ct);
        await secondHost.StartAsync(ct);

        try
        {
            var queueName =
                $"commands/long-work-job/subscription/{endpointName}";
            var queueStats = await WaitForQueueStatsAsync(
                queueName,
                stats => stats.ConsumerCount == 2,
                ct);
            Assert.Equal((uint)2, queueStats.ConsumerCount);

            await using var scope = firstHost.Services.CreateAsyncScope();
            var sender = scope.ServiceProvider
                .GetRequiredService<ICommandSender>();

            await sender.SendAsync(
                new StartLongWorkJobCommand(Guid.NewGuid()),
                token: ct);
            await sender.SendAsync(
                new StartLongWorkJobCommand(Guid.NewGuid()),
                token: ct);

            await probe.WaitUntilOneStartedAsync(ct);

            var blockedStats = await WaitForQueueStatsAsync(
                queueName,
                stats => stats.MessageCount == 1,
                ct);

            Assert.Equal((uint)1, blockedStats.MessageCount);
            Assert.Equal(1, probe.MaximumActive);

            probe.Release();
            await probe.WaitUntilAllCompletedAsync(ct);

            Assert.Equal(1, probe.MaximumActive);
        }
        finally
        {
            probe.Release();
            await StopHostAsync(secondHost);
            await StopHostAsync(firstHost);
        }
    }

    [Fact]
    public async Task SingleActiveConsumer_WhenActiveHostStops_PromotesStandbyConsumer()
    {
        var endpointName = CreateEndpointName();
        var firstProbe = new MessageProbe<StartLongWorkJobCommand>();
        var secondProbe = new MessageProbe<StartLongWorkJobCommand>();
        Action<RabbitMqConsumerOptions> configureConsumer = options =>
            options.ConfigureSubscription(
                "commands/long-work-job",
                subscription =>
                {
                    subscription.SingleActiveConsumer = true;
                    subscription.PrefetchCount = 1;
                    subscription.DispatcherConcurrency = 1;
                });
        using var firstHost = CreateHost(
            endpointName,
            services => services.AddSingleton(firstProbe),
            messaging => messaging
                .RegisterCommandHandler<
                    StartLongWorkJobCommand,
                    StartLongWorkJobCommandHandler>(),
            configureConsumer);
        using var secondHost = CreateHost(
            endpointName,
            services => services.AddSingleton(secondProbe),
            messaging => messaging
                .RegisterCommandHandler<
                    StartLongWorkJobCommand,
                    StartLongWorkJobCommandHandler>(),
            configureConsumer);
        using var publisherProvider = CreatePublisherProvider();
        using var cancellationTokenSource =
            new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var ct = cancellationTokenSource.Token;
        await firstHost.StartAsync(ct);
        await secondHost.StartAsync(ct);

        try
        {
            var queueName =
                $"commands/long-work-job/subscription/{endpointName}";
            await WaitForQueueStatsAsync(
                queueName,
                stats => stats.ConsumerCount == 2,
                ct);
            await using var scope = publisherProvider.CreateAsyncScope();
            var sender = scope.ServiceProvider
                .GetRequiredService<ICommandSender>();
            var firstCommand =
                new StartLongWorkJobCommand(Guid.NewGuid());
            var firstHostHandled = firstProbe.WaitAsync(ct);
            var secondHostHandled = secondProbe.WaitAsync(ct);

            await sender.SendAsync(firstCommand, token: ct);

            var activeProbeTask = await Task.WhenAny(
                firstHostHandled,
                secondHostHandled);
            var firstHandled = await activeProbeTask;
            Assert.Equal(firstCommand, firstHandled.Message);

            var firstHostWasActive =
                ReferenceEquals(activeProbeTask, firstHostHandled);
            var activeHost = firstHostWasActive
                ? firstHost
                : secondHost;
            var standbyProbeTask = firstHostWasActive
                ? secondHostHandled
                : firstHostHandled;

            await StopHostAsync(activeHost);
            await WaitForQueueStatsAsync(
                queueName,
                stats => stats.ConsumerCount == 1,
                ct);

            var secondCommand =
                new StartLongWorkJobCommand(Guid.NewGuid());
            await sender.SendAsync(secondCommand, token: ct);

            var secondHandled = await standbyProbeTask;

            Assert.Equal(secondCommand, secondHandled.Message);
        }
        finally
        {
            await StopHostAsync(secondHost);
            await StopHostAsync(firstHost);
        }
    }

    [Fact]
    public async Task HandlerFailure_PublishesMessageToDeadLetterQueueDeclaredAtStartup()
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
            var deadLetterQueueName =
                $"deadletter/events/clients/subscription/{endpointName}";
            var initialDeadLetterStats = await GetQueueStatsAsync(
                deadLetterQueueName,
                ct);

            Assert.Equal((uint)0, initialDeadLetterStats.MessageCount);
            Assert.Equal((uint)0, initialDeadLetterStats.ConsumerCount);

            await using var scope = host.Services.CreateAsyncScope();
            var publisher = scope.ServiceProvider
                .GetRequiredService<IEventPublisher>();

            await publisher.PublishAsync(
                new ClientCreatedEvent(Guid.NewGuid(), "Acme"),
                token: ct);

            var deadLetterStats = await WaitForQueueStatsAsync(
                deadLetterQueueName,
                stats => stats.MessageCount == 1,
                ct);

            Assert.Equal((uint)1, deadLetterStats.MessageCount);
            Assert.Equal((uint)0, deadLetterStats.ConsumerCount);
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
            var retryQueueName = "asyncomnia/retry-100ms-queue";
            var retryQueueStats = await GetQueueStatsAsync(
                retryQueueName,
                ct);
            Assert.Equal((uint)0, retryQueueStats.MessageCount);
            Assert.Equal((uint)0, retryQueueStats.ConsumerCount);

            await using var scope = host.Services.CreateAsyncScope();
            var publisher = scope.ServiceProvider
                .GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(
                new ClientCreatedEvent(Guid.NewGuid(), "Acme"),
                token: ct);

            var attempts = await probe.WaitForExpectedAttemptsAsync(ct);
            var deadLetterQueueName =
                $"deadletter/events/clients/subscription/{endpointName}";
            var deadLetterStats = await WaitForQueueStatsAsync(
                deadLetterQueueName,
                stats => stats.MessageCount == 1,
                ct);

            Assert.Equal(expectedAttempts, attempts);
            Assert.Equal(expectedAttempts, probe.Attempts);
            Assert.Equal((uint)1, deadLetterStats.MessageCount);

            retryQueueStats = await WaitForQueueStatsAsync(
                retryQueueName,
                stats => stats.MessageCount == 0,
                ct);
            Assert.Equal((uint)0, retryQueueStats.MessageCount);
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
            var deadLetterQueueName =
                $"deadletter/events/clients/subscription/{endpointName}";
            var deadLetterStats = await WaitForQueueStatsAsync(
                deadLetterQueueName,
                stats => stats.MessageCount == 1,
                ct);

            Assert.Equal(expectedAttempts, attempts);
            Assert.Equal(expectedAttempts, probe.Attempts);
            Assert.Equal((uint)1, deadLetterStats.MessageCount);
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

    private async Task<QueueDeclareOk> GetQueueStatsAsync(
        string queueName,
        CancellationToken ct)
    {
        await using var connection = await CreateConnectionAsync(ct);
        await using var channel = await connection.CreateChannelAsync(
            cancellationToken: ct);

        return await channel.QueueDeclarePassiveAsync(queueName, ct);
    }

    private async Task<QueueDeclareOk> WaitForQueueStatsAsync(
        string queueName,
        Func<QueueDeclareOk, bool> condition,
        CancellationToken ct)
    {
        await using var connection = await CreateConnectionAsync(ct);
        await using var channel = await connection.CreateChannelAsync(
            cancellationToken: ct);

        while (true)
        {
            var stats = await channel.QueueDeclarePassiveAsync(queueName, ct);

            if (condition(stats))
            {
                return stats;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
        }
    }

    private Task<IConnection> CreateConnectionAsync(CancellationToken ct)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(_connectionString)
        };

        return factory.CreateConnectionAsync(ct);
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
