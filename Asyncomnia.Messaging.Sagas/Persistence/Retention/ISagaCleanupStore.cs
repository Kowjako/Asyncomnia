namespace Asyncomnia.Messaging.Sagas.Persistence.Retention;

public interface ISagaCleanupStore
{
    Task<int> DeleteFinalizedAsync(TimeSpan finalizedSagaRetention, int batchSize, CancellationToken token = default);
}
