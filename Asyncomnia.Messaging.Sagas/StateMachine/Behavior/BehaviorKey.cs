namespace Asyncomnia.Messaging.Sagas.StateMachine.Behavior;

internal readonly record struct BehaviorKey(string StateName, Type MessageType);