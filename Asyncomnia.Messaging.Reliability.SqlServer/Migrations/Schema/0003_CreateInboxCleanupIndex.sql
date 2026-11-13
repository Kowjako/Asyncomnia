IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_Inbox_ReceivedAt' AND [object_id] = OBJECT_ID(N'[asyncomnia].[Inbox]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Inbox_ReceivedAt] ON [asyncomnia].[Inbox]
    (
        [ReceivedAt],
        [ConsumerId],
        [MessageId]
    );
END;