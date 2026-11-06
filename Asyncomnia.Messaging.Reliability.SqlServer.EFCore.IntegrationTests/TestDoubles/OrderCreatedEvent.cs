using Asyncomnia.Messaging;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.IntegrationTests.TestDoubles;

internal sealed record OrderCreatedEvent(Guid OrderId) : IEvent;
