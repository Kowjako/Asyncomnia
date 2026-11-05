namespace Asyncomnia.Messaging.Reliability.Tests.TestDoubles;

internal sealed record TestOutboxEvent(string Value) : IEvent;

internal abstract record TestBaseEvent : IEvent;

internal sealed record TestDerivedEvent(string Value) : TestBaseEvent;

internal sealed record TestOutboxCommand(string Value) : ICommand;
