using Asyncomnia.Messaging.Sagas.StateMachine.Configuration;

namespace Asyncomnia.Messaging.Sagas.StateMachine.Behavior;

public record BehaviorKey(State State, Type MessageType);