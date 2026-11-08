using Asyncomnia.Messaging.Retry;

namespace Asyncomnia.Messaging.Tests.Retry;

public sealed class InMemoryRetryOptionsTests
{
    [Fact]
    public void ByDefault_IsNotConfiguredAndHasNoDelays()
    {
        var options = new InMemoryRetryOptions();

        Assert.False(options.IsConfigured);
        Assert.Empty(options.Delays);
    }

    [Fact]
    public void Immediately_ConfiguresRequestedNumberOfRetriesWithoutDelay()
    {
        var options = new InMemoryRetryOptions();

        options.Immediately(3);

        Assert.True(options.IsConfigured);
        Assert.Equal(
            [TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero],
            options.Delays);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Immediately_WhenRetryCountIsNotPositive_Throws(
        int retryCount)
    {
        var options = new InMemoryRetryOptions();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => options.Immediately(retryCount));
    }

    [Fact]
    public void Intervals_PreservesOrderAndCopiesConfiguredDelays()
    {
        var intervals = new[]
        {
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromSeconds(1)
        };
        var options = new InMemoryRetryOptions();

        options.Intervals(intervals);
        intervals[0] = TimeSpan.FromDays(1);

        Assert.True(options.IsConfigured);
        Assert.Equal(
            [
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromSeconds(1)
            ],
            options.Delays);
    }

    [Fact]
    public void Intervals_WhenNull_Throws()
    {
        var options = new InMemoryRetryOptions();

        Assert.Throws<ArgumentNullException>(
            () => options.Intervals(null!));
    }

    [Fact]
    public void Intervals_WhenEmpty_Throws()
    {
        var options = new InMemoryRetryOptions();

        Assert.Throws<ArgumentException>(
            () => options.Intervals());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Intervals_WhenAnyIntervalIsNotPositive_Throws(
        int milliseconds)
    {
        var options = new InMemoryRetryOptions();

        Assert.Throws<ArgumentException>(
            () => options.Intervals(
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(milliseconds)));
    }

    [Fact]
    public void ConfigureStrategy_WhenStrategyWasAlreadyConfigured_Throws()
    {
        var options = new InMemoryRetryOptions();
        options.Immediately(1);

        Assert.Throws<InvalidOperationException>(
            () => options.Intervals(TimeSpan.FromSeconds(1)));
    }
}
