using Asyncomnia.Messaging.Sagas.StateMachine;

namespace Asyncomnia.Messaging.Sagas.Persistence;

public interface ISagaRepository<TSagaInstance> where TSagaInstance: AsyncomniaSagaInstance
{
    Task<TSagaInstance?> GetForUpdateAsync(Guid correlationId, CancellationToken token);
    Task<bool> TryInsertAsync(TSagaInstance instance, CancellationToken token);
    Task UpdateAsync(TSagaInstance instance, CancellationToken token);
}