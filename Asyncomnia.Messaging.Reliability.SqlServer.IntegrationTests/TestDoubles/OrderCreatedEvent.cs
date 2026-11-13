namespace Asyncomnia.Messaging.Reliability.SqlServer.IntegrationTests.TestDoubles;

internal sealed record OrderCreatedEvent(Guid OrderId) : IEvent;
