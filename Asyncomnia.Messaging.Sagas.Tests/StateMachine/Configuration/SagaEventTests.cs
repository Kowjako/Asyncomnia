using Asyncomnia.Messaging.Sagas.StateMachine.Configuration;
using Asyncomnia.Messaging.Sagas.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Sagas.Tests.StateMachine.Configuration;

public sealed class SagaEventTests
{
    [Fact]
    public void CorrelateById_ReturnsSameConfiguration()
    {
        var configuration = new SagaEventConfiguration<TestEvent>();

        var result = configuration.CorrelateById(message => message.SagaId);

        Assert.Same(configuration, result);
    }

    [Fact]
    public void CorrelateById_WhenSelectorIsNull_ThrowsArgumentNullException()
    {
        var configuration = new SagaEventConfiguration<TestEvent>();

        var exception = Assert.Throws<ArgumentNullException>(
            () => configuration.CorrelateById(null!));

        Assert.Equal("correlationIdSelector", exception.ParamName);
    }

    [Fact]
    public void CorrelateByMessageCorrelationId_ReturnsSameConfiguration()
    {
        var configuration = new SagaEventConfiguration<ScheduledTimeout>();

        var result = configuration.CorrelateByMessageCorrelationId();

        Assert.Same(configuration, result);
    }

    [Fact]
    public void Build_WithoutCorrelationSelector_ThrowsInvalidOperationException()
    {
        var configuration = new SagaEventConfiguration<TestEvent>();

        var exception = Assert.Throws<InvalidOperationException>(() => configuration.Build());

        Assert.Contains(typeof(TestEvent).FullName!, exception.Message);
        Assert.Contains(nameof(SagaEventConfiguration<TestEvent>.CorrelateById), exception.Message);
        Assert.Contains(
            nameof(SagaEventConfiguration<TestEvent>.CorrelateByMessageCorrelationId),
            exception.Message);
    }

    [Fact]
    public void Build_WithPayloadCorrelationSelector_CreatesEventForMessageType()
    {
        var configuration = new SagaEventConfiguration<TestEvent>()
            .CorrelateById(message => message.SagaId);

        var sagaEvent = configuration.Build();

        Assert.Equal(typeof(TestEvent), sagaEvent.Type);
    }

    [Fact]
    public void Build_WithMetadataCorrelationSelector_CreatesEventForMessageType()
    {
        var configuration = new SagaEventConfiguration<ScheduledTimeout>()
            .CorrelateByMessageCorrelationId();

        var sagaEvent = configuration.Build();

        Assert.Equal(typeof(ScheduledTimeout), sagaEvent.Type);
    }

    [Fact]
    public void Constructor_WhenSelectorIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(
            () => new SagaEvent<TestEvent>(null!));

        Assert.Equal("correlationIdSelector", exception.ParamName);
    }

    [Fact]
    public void GetCorrelationId_WithPayloadSelector_ReturnsValueFromMessage()
    {
        var correlationId = Guid.NewGuid();
        var message = new TestEvent(correlationId);
        var sagaEvent = new SagaEvent<TestEvent>((value, _) => value.SagaId);

        var result = sagaEvent.GetCorrelationId(message, CreateMetadata(Guid.NewGuid()));

        Assert.Equal(correlationId, result);
    }

    [Fact]
    public void GetCorrelationId_WithMessageCorrelationSelector_ReturnsValueFromMetadata()
    {
        var correlationId = Guid.NewGuid();
        var sagaEvent = new SagaEventConfiguration<ScheduledTimeout>()
            .CorrelateByMessageCorrelationId()
            .Build();

        var result = sagaEvent.GetCorrelationId(
            new ScheduledTimeout(),
            CreateMetadata(correlationId));

        Assert.Equal(correlationId, result);
    }

    [Fact]
    public void GetCorrelationId_WithMessageCorrelationSelectorAndMissingCorrelationId_Throws()
    {
        var sagaEvent = new SagaEventConfiguration<ScheduledTimeout>()
            .CorrelateByMessageCorrelationId()
            .Build();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            sagaEvent.GetCorrelationId(new ScheduledTimeout(), CreateMetadata()));

        Assert.Contains(typeof(ScheduledTimeout).FullName!, exception.Message);
        Assert.Contains("does not contain a correlation ID", exception.Message);
    }

    [Fact]
    public void GetCorrelationId_WhenMessageIsNull_ThrowsArgumentNullException()
    {
        var sagaEvent = new SagaEvent<TestEvent>((value, _) => value.SagaId);

        var exception = Assert.Throws<ArgumentNullException>(
            () => sagaEvent.GetCorrelationId(null!, CreateMetadata()));

        Assert.Equal("message", exception.ParamName);
    }

    [Fact]
    public void GetCorrelationId_WhenMetadataIsNull_ThrowsArgumentNullException()
    {
        var sagaEvent = new SagaEvent<TestEvent>((value, _) => value.SagaId);

        var exception = Assert.Throws<ArgumentNullException>(() =>
            sagaEvent.GetCorrelationId(new TestEvent(Guid.NewGuid()), null!));

        Assert.Equal("metadata", exception.ParamName);
    }

    [Fact]
    public void GetCorrelationId_WhenMessageHasDifferentType_ThrowsArgumentException()
    {
        var sagaEvent = new SagaEvent<TestEvent>((value, _) => value.SagaId);

        var exception = Assert.Throws<ArgumentException>(() =>
            sagaEvent.GetCorrelationId(
                new SecondTestEvent(Guid.NewGuid()),
                CreateMetadata()));

        Assert.Equal("message", exception.ParamName);
        Assert.Contains(typeof(TestEvent).FullName!, exception.Message);
        Assert.Contains(typeof(SecondTestEvent).FullName!, exception.Message);
    }

    [Fact]
    public void GetCorrelationId_WhenPayloadSelectorReturnsEmptyGuid_ThrowsInvalidOperationException()
    {
        var sagaEvent = new SagaEvent<TestEvent>((_, _) => Guid.Empty);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            sagaEvent.GetCorrelationId(
                new TestEvent(Guid.NewGuid()),
                CreateMetadata()));

        Assert.Contains(typeof(TestEvent).FullName!, exception.Message);
        Assert.Contains(nameof(Guid.Empty), exception.Message);
    }

    [Fact]
    public void GetCorrelationId_WhenMetadataContainsEmptyGuid_ThrowsInvalidOperationException()
    {
        var sagaEvent = new SagaEventConfiguration<ScheduledTimeout>()
            .CorrelateByMessageCorrelationId()
            .Build();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            sagaEvent.GetCorrelationId(
                new ScheduledTimeout(),
                CreateMetadata(Guid.Empty)));

        Assert.Contains(typeof(ScheduledTimeout).FullName!, exception.Message);
        Assert.Contains(nameof(Guid.Empty), exception.Message);
    }

    private static MessageMetadata CreateMetadata(Guid? correlationId = null)
    {
        return new MessageMetadata(Guid.NewGuid(), DateTimeOffset.UtcNow)
        {
            CorrelationId = correlationId
        };
    }
}
