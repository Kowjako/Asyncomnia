using Asyncomnia.Messaging.Configuration;
using Asyncomnia.Messaging.Sagas.Configuration;
using Asyncomnia.Messaging.Sagas.DependencyInjection;
using Asyncomnia.Messaging.Sagas.StateMachine;
using Asyncomnia.Messaging.Sagas.Tests.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Asyncomnia.Messaging.Sagas.Tests.Configuration;

public sealed class SagaOptionsValidatorTests
{
    [Fact]
    public void Validate_WithoutInboxAndOutbox_ReturnsBothFailures()
    {
        var validator = new SagaOptionsValidator(new StubServiceProviderIsService());

        var result = validator.Validate(null, new SagaOptions());

        Assert.True(result.Failed);
        Assert.Equal(2, result.Failures.Count());
        Assert.Contains(result.Failures, failure => failure.Contains("UseInbox()"));
        Assert.Contains(result.Failures, failure => failure.Contains("UseOutbox()"));
    }

    [Fact]
    public void Validate_WithOnlyInbox_ReturnsOutboxFailure()
    {
        var services = new StubServiceProviderIsService(typeof(InboxFeatureRegistration));
        var validator = new SagaOptionsValidator(services);

        var result = validator.Validate(null, new SagaOptions());

        Assert.True(result.Failed);
        var failure = Assert.Single(result.Failures);
        Assert.Contains("UseOutbox()", failure);
    }

    [Fact]
    public void Validate_WithOnlyOutbox_ReturnsInboxFailure()
    {
        var services = new StubServiceProviderIsService(typeof(OutboxFeatureRegistration));
        var validator = new SagaOptionsValidator(services);

        var result = validator.Validate(null, new SagaOptions());

        Assert.True(result.Failed);
        var failure = Assert.Single(result.Failures);
        Assert.Contains("UseInbox()", failure);
    }

    [Fact]
    public void Validate_WithInboxAndOutbox_ReturnsSuccess()
    {
        var services = new StubServiceProviderIsService(
            typeof(InboxFeatureRegistration),
            typeof(OutboxFeatureRegistration));
        var validator = new SagaOptionsValidator(services);

        var result = validator.Validate(null, new SagaOptions());

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void Validate_WhenFinalizedSagaRetentionIsNotPositive_ReturnsFailure()
    {
        var validator = CreateValidatorWithRequiredFeatures();
        var options = new SagaOptions
        {
            FinalizedSagaRetention = TimeSpan.Zero
        };

        var result = validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains(
            "SagaOptions:FinalizedSagaRetention must be greater than zero",
            result.Failures);
    }

    [Fact]
    public void Validate_WhenCleanupBatchSizeIsNotPositive_ReturnsFailure()
    {
        var validator = CreateValidatorWithRequiredFeatures();
        var options = new SagaOptions
        {
            CleanupBatchSize = 0
        };

        var result = validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains(
            "SagaOptions:CleanupBatchSize must be greater than zero",
            result.Failures);
    }

    [Fact]
    public void Validate_WhenCleanupIntervalIsNotPositive_ReturnsFailure()
    {
        var validator = CreateValidatorWithRequiredFeatures();
        var options = new SagaOptions
        {
            CleanupInterval = TimeSpan.Zero
        };

        var result = validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains(
            "SagaOptions:CleanupInterval must be greater than zero",
            result.Failures);
    }

    [Fact]
    public void RegisteredSaga_WithoutInboxAndOutbox_StartupValidationThrowsOptionsValidationException()
    {
        var services = new ServiceCollection();
        new AsyncomniaMessagingBuilder(services)
            .RegisterAsyncomniaSaga<MinimalStateMachine, TestSagaInstance>();
        using var provider = services.BuildServiceProvider();
        var startupValidator = provider.GetRequiredService<IStartupValidator>();

        var exception = Assert.Throws<OptionsValidationException>(() =>
            startupValidator.Validate());

        Assert.Equal(2, exception.Failures.Count());
        Assert.Contains(exception.Failures, failure => failure.Contains("UseInbox()"));
        Assert.Contains(exception.Failures, failure => failure.Contains("UseOutbox()"));
    }

    [Fact]
    public void RegisteredSaga_WithInboxAndOutbox_StartupValidationSucceeds()
    {
        var services = new ServiceCollection();
        services.AddSingleton<InboxFeatureRegistration>();
        services.AddSingleton<OutboxFeatureRegistration>();
        new AsyncomniaMessagingBuilder(services)
            .RegisterAsyncomniaSaga<MinimalStateMachine, TestSagaInstance>();
        using var provider = services.BuildServiceProvider();
        var startupValidator = provider.GetRequiredService<IStartupValidator>();

        var exception = Record.Exception(startupValidator.Validate);

        Assert.Null(exception);
    }

    private sealed class StubServiceProviderIsService(params Type[] registeredServices)
        : IServiceProviderIsService
    {
        private readonly HashSet<Type> _registeredServices = registeredServices.ToHashSet();

        public bool IsService(Type serviceType)
        {
            return _registeredServices.Contains(serviceType);
        }
    }

    private static SagaOptionsValidator CreateValidatorWithRequiredFeatures()
    {
        return new SagaOptionsValidator(
            new StubServiceProviderIsService(
                typeof(InboxFeatureRegistration),
                typeof(OutboxFeatureRegistration)));
    }

    private sealed class MinimalStateMachine : AsyncomniaStateMachine<TestSagaInstance>
    {
        public MinimalStateMachine()
        {
            Event<TestEvent>(configuration =>
                configuration.CorrelateById(message => message.SagaId));
        }
    }
}
