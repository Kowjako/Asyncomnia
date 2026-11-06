namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.TestDoubles;

internal sealed class TestOrder
{
    public Guid Id { get; private set; }

    public string Description { get; private set; } = null!;

    private TestOrder()
    {
    }

    public TestOrder(Guid id, string description)
    {
        Id = id;
        Description = description;
    }
}
