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
    public void Build_WithoutCorrelationSelector_ThrowsInvalidOperationException()
    {
        var configuration = new SagaEventConfiguration<TestEvent>();

        var exception = Assert.Throws<InvalidOperationException>(() => configuration.Build());

        Assert.Contains(typeof(TestEvent).FullName!, exception.Message);
        Assert.Contains("CorrelateById", exception.Message);
    }

    [Fact]
    public void Build_WithCorrelationSelector_CreatesEventForMessageType()
    {
        var configuration = new SagaEventConfiguration<TestEvent>()
            .CorrelateById(message => message.SagaId);

        var sagaEvent = configuration.Build();

        Assert.Equal(typeof(TestEvent), sagaEvent.Type);
    }

    [Fact]
    public void Constructor_WhenSelectorIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(
            () => new SagaEvent<TestEvent>(null!));

        Assert.Equal("correlateByIdSelector", exception.ParamName);
    }

    [Fact]
    public void GetCorrelationId_ReturnsValueFromSelector()
    {
        var correlationId = Guid.NewGuid();
        var message = new TestEvent(correlationId);
        var sagaEvent = new SagaEvent<TestEvent>(value => value.SagaId);

        var result = sagaEvent.GetCorrelationId(message);

        Assert.Equal(correlationId, result);
    }

    [Fact]
    public void GetCorrelationId_WhenMessageIsNull_ThrowsArgumentNullException()
    {
        var sagaEvent = new SagaEvent<TestEvent>(value => value.SagaId);

        var exception = Assert.Throws<ArgumentNullException>(
            () => sagaEvent.GetCorrelationId(null!));

        Assert.Equal("message", exception.ParamName);
    }

    [Fact]
    public void GetCorrelationId_WhenMessageHasDifferentType_ThrowsArgumentException()
    {
        var sagaEvent = new SagaEvent<TestEvent>(value => value.SagaId);

        var exception = Assert.Throws<ArgumentException>(
            () => sagaEvent.GetCorrelationId(new SecondTestEvent(Guid.NewGuid())));

        Assert.Equal("message", exception.ParamName);
        Assert.Contains(typeof(TestEvent).FullName!, exception.Message);
        Assert.Contains(typeof(SecondTestEvent).FullName!, exception.Message);
    }

    [Fact]
    public void GetCorrelationId_WhenSelectorReturnsEmptyGuid_ThrowsInvalidOperationException()
    {
        var sagaEvent = new SagaEvent<TestEvent>(_ => Guid.Empty);

        var exception = Assert.Throws<InvalidOperationException>(
            () => sagaEvent.GetCorrelationId(new TestEvent(Guid.NewGuid())));

        Assert.Contains(typeof(TestEvent).FullName!, exception.Message);
        Assert.Contains(nameof(Guid.Empty), exception.Message);
    }
}
