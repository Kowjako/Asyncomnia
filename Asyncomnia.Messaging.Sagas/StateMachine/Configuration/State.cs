namespace Asyncomnia.Messaging.Sagas.StateMachine.Configuration;

public sealed record State
{
    public string Name { get; }

    public static readonly State Initial = new("Initial");
    public static readonly State Final = new("Final");

    public State(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        Name = name;
    }
}