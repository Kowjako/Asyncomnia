using Asyncomnia.Messaging.Routing;

namespace Asyncomnia.Messaging.RabbitMQ.IntegrationTests.TestDoubles;

[MessageRoute("clients")]
internal sealed record ClientCreatedEvent(Guid ClientId, string Name) : IEvent;

[MessageRoute("clients", "client-contract-signed")]
internal sealed record ClientContractSignedEvent(Guid ClientId, string ContractNumber) : IEvent;

[MessageRoute("unhandled-jobs")]
internal sealed record StartUnhandledJobCommand(Guid JobId) : ICommand;

[MessageRoute("long-work-job")]
internal sealed record StartLongWorkJobCommand(Guid JobId) : ICommand;
