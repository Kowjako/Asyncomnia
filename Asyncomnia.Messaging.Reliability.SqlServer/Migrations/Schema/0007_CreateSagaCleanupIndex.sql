IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_Saga_FinalizedCleanup'
      AND [object_id] = OBJECT_ID(N'[asyncomnia].[Saga]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Saga_FinalizedCleanup]
        ON [asyncomnia].[Saga]
        (
            [UpdatedAt]
        )
        WHERE [CurrentState] = N'Final';
END;
