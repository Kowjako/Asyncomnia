using System.Text.Json;
using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.DependencyInjection;
using Asyncomnia.Messaging.Reliability.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace Asyncomnia.Messaging.Reliability.Tests.DelayedPublishing;

public sealed class MessageSchedulerTests
{
    [Fact]
    public async Task ScheduleAsync_WithAbsoluteDate_WritesSerializedMessageAndReturnsScheduleId()
    {
        var utcNow = new DateTimeOffset(2026, 11, 15, 10, 0, 0, TimeSpan.Zero);
        var deliverAt = utcNow.AddHours(3);
        var correlationId = Guid.NewGuid();
        DelayedMessage? writtenMessage = null;
        CancellationToken writtenToken = default;
        var writer = CreateWriter((message, token) =>
        {
            writtenMessage = message;
            writtenToken = token;
        });
        await using var provider = CreateProvider(utcNow, writer.Object);
        await using var scope = provider.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var message = new TestOutboxCommand("generate-report");
        using var cancellation = new CancellationTokenSource();

        var scheduleId = await scheduler.ScheduleAsync(
            message,
            deliverAt,
            new MessageDispatchOptions { CorrelationId = correlationId },
            cancellation.Token);

        var delayedMessage = Assert.IsType<DelayedMessage>(writtenMessage);
        Assert.NotEqual(Guid.Empty, scheduleId);
        Assert.Equal(scheduleId, delayedMessage.ScheduleId);
        Assert.NotEqual(Guid.Empty, delayedMessage.MessageId);
        Assert.Equal(GetContractType(typeof(TestOutboxCommand)), delayedMessage.ContractType);
        Assert.Equal(correlationId, delayedMessage.CorrelationId);
        Assert.Equal(utcNow, delayedMessage.CreatedAt);
        Assert.Equal(deliverAt, delayedMessage.DeliverAt);
        Assert.Equal(cancellation.Token, writtenToken);

        using var json = JsonDocument.Parse(delayedMessage.MessageJson);
        Assert.Equal(message.Value, json.RootElement.GetProperty("value").GetString());
        writer.VerifyAll();
    }

    [Fact]
    public async Task ScheduleAsync_WithDelay_CalculatesDeliverAtUsingTimeProvider()
    {
        var utcNow = new DateTimeOffset(2026, 11, 15, 10, 0, 0, TimeSpan.Zero);
        var delay = TimeSpan.FromMinutes(45);
        DelayedMessage? writtenMessage = null;
        var writer = CreateWriter((message, _) => writtenMessage = message);
        await using var provider = CreateProvider(utcNow, writer.Object);
        await using var scope = provider.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();

        await scheduler.ScheduleAsync(new TestOutboxEvent("event"), delay);

        Assert.Equal(utcNow + delay, Assert.IsType<DelayedMessage>(writtenMessage).DeliverAt);
    }

    [Fact]
    public async Task ScheduleAsync_WhenVariableUsesBaseType_StoresRuntimeContractType()
    {
        DelayedMessage? writtenMessage = null;
        var writer = CreateWriter((message, _) => writtenMessage = message);
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, writer.Object);
        await using var scope = provider.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        TestBaseEvent message = new TestDerivedEvent("derived-event");

        await scheduler.ScheduleAsync(message, DateTimeOffset.UtcNow.AddMinutes(1));

        Assert.Equal(
            GetContractType(typeof(TestDerivedEvent)),
            Assert.IsType<DelayedMessage>(writtenMessage).ContractType);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task ScheduleAsync_WhenDelayIsNotPositive_ThrowsArgumentOutOfRangeException(
        int delayMilliseconds)
    {
        var writer = new Mock<IDelayedMessageWriter>(MockBehavior.Strict);
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, writer.Object);
        await using var scope = provider.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            scheduler.ScheduleAsync(
                new TestOutboxEvent("event"),
                TimeSpan.FromMilliseconds(delayMilliseconds)));
    }

    [Fact]
    public async Task ScheduleAsync_WhenMessageIsNull_ThrowsArgumentNullException()
    {
        var writer = new Mock<IDelayedMessageWriter>(MockBehavior.Strict);
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, writer.Object);
        await using var scope = provider.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            scheduler.ScheduleAsync<TestOutboxEvent>(
                null!,
                DateTimeOffset.UtcNow.AddMinutes(1)));
    }

    [Fact]
    public async Task ScheduleAsync_WhenWriterFails_PropagatesException()
    {
        var expectedException = new InvalidOperationException("Write failed");
        var writer = new Mock<IDelayedMessageWriter>(MockBehavior.Strict);
        writer
            .Setup(x => x.WriteAsync(
                It.IsAny<DelayedMessage>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(expectedException);
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, writer.Object);
        await using var scope = provider.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            scheduler.ScheduleAsync(
                new TestOutboxEvent("event"),
                DateTimeOffset.UtcNow.AddMinutes(1)));

        Assert.Same(expectedException, exception);
    }

    [Fact]
    public async Task CancelAsync_DelegatesScheduleIdAndCancellationTokenToWriter()
    {
        var scheduleId = Guid.NewGuid();
        using var cancellation = new CancellationTokenSource();
        var writer = new Mock<IDelayedMessageWriter>(MockBehavior.Strict);
        writer
            .Setup(x => x.CancelAsync(scheduleId, cancellation.Token))
            .ReturnsAsync(true);
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, writer.Object);
        await using var scope = provider.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();

        var cancelled = await scheduler.CancelAsync(scheduleId, cancellation.Token);

        Assert.True(cancelled);
        writer.VerifyAll();
    }

    [Fact]
    public async Task CancelAsync_WhenScheduleIdIsEmpty_ThrowsArgumentException()
    {
        var writer = new Mock<IDelayedMessageWriter>(MockBehavior.Strict);
        await using var provider = CreateProvider(DateTimeOffset.UtcNow, writer.Object);
        await using var scope = provider.CreateAsyncScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();

        await Assert.ThrowsAsync<ArgumentException>(() =>
            scheduler.CancelAsync(Guid.Empty));
    }

    private static Mock<IDelayedMessageWriter> CreateWriter(
        Action<DelayedMessage, CancellationToken> onWrite)
    {
        var writer = new Mock<IDelayedMessageWriter>(MockBehavior.Strict);
        writer
            .Setup(x => x.WriteAsync(
                It.IsAny<DelayedMessage>(),
                It.IsAny<CancellationToken>()))
            .Callback(onWrite)
            .Returns(Task.CompletedTask);

        return writer;
    }

    private static ServiceProvider CreateProvider(
        DateTimeOffset utcNow,
        IDelayedMessageWriter writer)
    {
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(new FixedTimeProvider(utcNow));
        services.AddSingleton(writer);
        services.AddAsyncomniaMessaging().UseDelayedPublishing();

        return services.BuildServiceProvider();
    }

    private static string GetContractType(Type messageType)
    {
        return $"{messageType.FullName}, {messageType.Assembly.GetName().Name}";
    }
}
