using Asyncomnia.Messaging.Sagas.StateMachine;
using Asyncomnia.Messaging.Sagas.StateMachine.Configuration;

namespace Asyncomnia.Messaging.Sagas;

public class PaymentStartedEvent : IEvent
{
    public Guid CorrelationId { get; set; }
}

public class PaymentCompletedEvent : IEvent
{
    public Guid CorrelationId { get; set; }
}

public class ExampleBody : AsyncomniaSagaInstance
{
    public int Amount { get; set; }
}

public class ExampleSaga : AsyncomniaStateMachine<ExampleBody>
{
    public State PaymentReceived { get; }
    public State PaymentCompleted { get; }

    public SagaEvent<PaymentStartedEvent> PaymentStartedEvent { get; }

    public SagaEvent<PaymentCompletedEvent> PaymentCompletedEvent { get; }
    
    public ExampleSaga()
    {
        PaymentReceived = State(nameof(PaymentReceived));
        PaymentCompleted = State(nameof(PaymentCompleted));

        PaymentStartedEvent = Event<PaymentStartedEvent>(config => config.CorrelateById(x => x.CorrelationId));
        PaymentCompletedEvent = Event<PaymentCompletedEvent>(config => config.CorrelateById(x => x.CorrelationId));

        Initially(
            When(PaymentStartedEvent)
                .Then(c =>
                {
                    c.State.Amount = 100;
                })
                .TransitionTo(PaymentReceived),
            When(PaymentCompletedEvent)
                .Finalize());

        During(PaymentReceived,
            When(PaymentStartedEvent)
                .Then(c =>
                {
                    c.State.Amount = 100;
                })
                .TransitionTo(PaymentCompleted),
            When(PaymentCompletedEvent)
                .Finalize());

        During(PaymentReceived,
            When(PaymentStartedEvent)
                .If(ctx => ctx.State.Amount > 0,
                        builder => builder.TransitionTo(PaymentReceived)));
    }
}
