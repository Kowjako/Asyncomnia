using Asyncomnia.Messaging.Sagas.Persistence;
using Asyncomnia.Messaging.Sagas.StateMachine;

namespace Asyncomnia.Messaging.Sagas.Runtime;

internal sealed class SagaRuntime<TStateMachine, TSagaInstance> 
	where TStateMachine: AsyncomniaStateMachine<TSagaInstance>
	where TSagaInstance: AsyncomniaSagaInstance, new()
{
	private readonly ISagaRepository<TSagaInstance> _sagaRepository;
	private readonly TStateMachine _stateMachine;
	private readonly IServiceProvider _serviceProvider;

	public SagaRuntime(ISagaRepository<TSagaInstance> sagaRepository,
		TStateMachine stateMachine, IServiceProvider serviceProvider)
	{
		_sagaRepository = sagaRepository ?? throw new ArgumentNullException(nameof(sagaRepository));
		_stateMachine = stateMachine ?? throw new ArgumentNullException(nameof(stateMachine));
		_serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    }

    public async Task HandleAsync(object message, MessageMetadata metadata, CancellationToken token = default)
	{
		ArgumentNullException.ThrowIfNull(message);
		ArgumentNullException.ThrowIfNull(metadata);

		if (!_stateMachine.TryGetEvent(message.GetType(), out var sagaEvent) || sagaEvent is null)
		{
            throw new InvalidOperationException($"State machine '{typeof(TStateMachine).FullName}' does not define an event for message type '{message.GetType().FullName}'.");
        }

		var correlationId = sagaEvent.GetCorrelationId(message, metadata);

		var instance = await _sagaRepository.GetForUpdateAsync(correlationId, token);
		if (instance is null)
		{
            instance = new TSagaInstance
			{
				CorrelationId = correlationId
			};

			var inserted = await _sagaRepository.TryInsertAsync(instance, token);
			if (!inserted)
			{
                // Here we come, if another thread at the same time inserted SagaInstance, and our thread lose insert (due to PK conflict)
                // so we can grab it now and process for current message
                instance = await _sagaRepository.GetForUpdateAsync(correlationId, token);
				if (instance == null)
				{
                    throw new InvalidOperationException($"Saga repository reported an insert conflict for saga instance '{typeof(TSagaInstance).FullName}' with correlation ID '{correlationId}', but the instance could not be loaded afterwards.");
				}
			}
		}

		if (!_stateMachine.TryGetBehavior(instance.CurrentState, message.GetType(), out var behavior) || behavior is null)
		{
            throw new InvalidOperationException($"State machine '{typeof(TStateMachine).FullName}' does not define a behavior for message type '{message.GetType().FullName}' in state '{instance.CurrentState}' for saga instance with correlation ID '{correlationId}'.");
        }

		await behavior.ExecuteAsync(instance, message, _serviceProvider, token);
		await _sagaRepository.UpdateAsync(instance, token);
    }
}