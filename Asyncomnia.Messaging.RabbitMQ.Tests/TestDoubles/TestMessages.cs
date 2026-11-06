using Asyncomnia.Messaging.Routing;

namespace Asyncomnia.Messaging.RabbitMQ.Tests.TestDoubles;

[MessageRoute("clients")]
internal sealed record ClientCreatedEvent(string ClientName) : IEvent;

[MessageRoute("clients", "client-contract-signed")]
internal sealed record ClientContractSignedEvent(Guid ClientId) : IEvent;

[MessageRoute("long-work-job")]
internal sealed record StartLongWorkJobCommand(Guid JobId) : ICommand;

[MessageRoute("http")]
internal sealed record HTTPResponseReceivedEvent(int StatusCode) : IEvent;

[MessageRoute("invalid")]
internal sealed record AmbiguousMessage : IEvent, ICommand;

[MessageRoute("invalid")]
internal sealed record MessageWithoutKind : IMessage;

internal sealed record MessageWithoutRoute : IEvent;

internal sealed record NonAsyncomniaMessage;
