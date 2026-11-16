namespace Asyncomnia.Messaging.Configuration;

// Marker which is avabilable in DI, when UseInbox was called and inbox was configured
internal sealed class InboxFeatureRegistration;

// Marker which is avaialble in DI, when UseOutbox was called and outbox was configured
internal sealed class OutboxFeatureRegistration;
