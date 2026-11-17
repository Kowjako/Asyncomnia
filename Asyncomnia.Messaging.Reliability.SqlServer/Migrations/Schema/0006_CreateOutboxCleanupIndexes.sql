IF NOT EXISTS
(
    SELECT 1 FROM sys.indexes
    WHERE [name] = N'IX_Outbox_ProcessedAt' AND [object_id] = OBJECT_ID(N'[asyncomnia].[Outbox]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Outbox_ProcessedAt] ON [asyncomnia].[Outbox]
    (
        [ProcessedAt],
        [MessageId]
    )
    WHERE [ProcessedAt] IS NOT NULL;
END;

IF NOT EXISTS
(
    SELECT 1 FROM sys.indexes
    WHERE [name] = N'IX_Outbox_FailedAt' AND [object_id] = OBJECT_ID(N'[asyncomnia].[Outbox]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Outbox_FailedAt] ON [asyncomnia].[Outbox]
    (
        [FailedAt],
        [MessageId]
    )
    WHERE [FailedAt] IS NOT NULL;
END;