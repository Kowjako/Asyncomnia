using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.DelayedPublishing;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Outbox;
using Microsoft.EntityFrameworkCore;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Extensions;

public static class ModelBuilderExtensions
{
    public static ModelBuilder AddAsyncomniaOutbox(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.ApplyConfiguration(new OutboxEntryConfiguration());

        return modelBuilder;
    }

    public static ModelBuilder AddAsyncomniaDelayedPublishing(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.ApplyConfiguration(new DelayedMessageEntryConfiguration());

        return modelBuilder;
    }
}