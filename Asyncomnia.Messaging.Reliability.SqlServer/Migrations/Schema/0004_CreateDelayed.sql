IF OBJECT_ID(N'[asyncomnia].[Delayed]', N'U') IS NULL
BEGIN
    CREATE TABLE [asyncomnia].[Delayed]
    (
        [ScheduleId] uniqueidentifier NOT NULL,
        [MessageId] uniqueidentifier NOT NULL,
        [ContractType] nvarchar(1000) NOT NULL,
        [MessageJson] nvarchar(max) NOT NULL,
        [CorrelationId] uniqueidentifier NULL,
        [CreatedAt] datetimeoffset(7) NOT NULL,
        [DeliverAt] datetimeoffset(7) NOT NULL,

        CONSTRAINT [PK_Delayed] PRIMARY KEY CLUSTERED ([ScheduleId]),

        CONSTRAINT [UQ_Delayed_MessageId] UNIQUE ([MessageId])
    );
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_Delayed_Due' AND [object_id] = OBJECT_ID(N'[asyncomnia].[Delayed]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Delayed_Due] ON [asyncomnia].[Delayed]
    (
        [DeliverAt],
        [ScheduleId]
    );
END;