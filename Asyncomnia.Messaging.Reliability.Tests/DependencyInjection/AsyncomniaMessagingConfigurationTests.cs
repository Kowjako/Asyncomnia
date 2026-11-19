using Asyncomnia.Messaging.DependencyInjection;
using Asyncomnia.Messaging.Reliability.DelayedPublishing.Configuration;
using Asyncomnia.Messaging.Reliability.Inbox.Configuration;
using Asyncomnia.Messaging.Reliability.Outbox.Configuration;
using Asyncomnia.Messaging.Reliability.Outbox.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;

namespace Asyncomnia.Messaging.Reliability.Tests.DependencyInjection;

public sealed class AsyncomniaMessagingConfigurationTests
{
    [Fact]
    public void AddAsyncomniaMessaging_WithReliabilityFeatures_AppliesAllConfiguration()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Mock.Of<IOutboxStore>());
        services.AddSingleton(Mock.Of<IOutboxCleanupStore>());

        services.AddAsyncomniaMessaging(messaging =>
        {
            messaging.UseInbox(options =>
            {
                options.DuplicateDetectionWindow = TimeSpan.FromHours(12);
            });

            messaging.UseOutbox(options =>
            {
                options.WorkerCount = 3;
            });

            messaging.UseDelayedPublishing(options =>
            {
                options.BatchSize = 75;
            });
        });

        using var provider = services.BuildServiceProvider();

        var inbox = provider.GetRequiredService<IOptions<InboxOptions>>().Value;
        var outbox = provider.GetRequiredService<IOptions<OutboxOptions>>().Value;
        var delayed = provider
            .GetRequiredService<IOptions<DelayedPublishingOptions>>()
            .Value;

        Assert.Equal(TimeSpan.FromHours(12), inbox.DuplicateDetectionWindow);
        Assert.Equal(3, outbox.WorkerCount);
        Assert.Equal(75, delayed.BatchSize);
    }
}
