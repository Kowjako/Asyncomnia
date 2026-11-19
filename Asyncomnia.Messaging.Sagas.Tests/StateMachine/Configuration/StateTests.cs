using Asyncomnia.Messaging.Sagas.StateMachine.Configuration;

namespace Asyncomnia.Messaging.Sagas.Tests.StateMachine.Configuration;

public sealed class StateTests
{
    [Fact]
    public void Constructor_WithName_SetsName()
    {
        var state = new State("Submitted");

        Assert.Equal("Submitted", state.Name);
    }

    [Fact]
    public void Constructor_WhenNameIsNull_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() => new State(null!));

        Assert.Equal("name", exception.ParamName);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void Constructor_WhenNameIsEmptyOrWhitespace_ThrowsArgumentException(string name)
    {
        var exception = Assert.Throws<ArgumentException>(() => new State(name));

        Assert.Equal("name", exception.ParamName);
    }

    [Fact]
    public void States_WithSameName_AreEqual()
    {
        var first = new State("Submitted");
        var second = new State("Submitted");

        Assert.Equal(first, second);
    }

    [Fact]
    public void BuiltInStates_HaveExpectedNames()
    {
        Assert.Equal("Initial", State.Initial.Name);
        Assert.Equal("Final", State.Final.Name);
        Assert.NotEqual(State.Initial, State.Final);
    }
}
