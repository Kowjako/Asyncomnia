namespace Asyncomnia.Messaging.Reliability.SqlServer.Dapper.IntegrationTests.TestDoubles;

internal sealed record OrderCreatedEvent(Guid OrderId) : IEvent;