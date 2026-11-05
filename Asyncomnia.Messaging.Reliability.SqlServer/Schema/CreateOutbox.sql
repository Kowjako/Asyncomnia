IF SCHEMA_ID(N'asyncomnia') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA [asyncomnia]');
END;

IF OBJECT_ID(N'[asyncomnia].[Outbox]', N'U') IS NULL
BEGIN
    CREATE TABLE [asyncomnia].[Outbox]
    (
        [MessageId] uniqueidentifier NOT NULL,
        [ContractType] nvarchar(1000) NOT NULL,
        [MessageJson] nvarchar(max) NOT NULL,
        [CorrelationId] uniqueidentifier NULL,
        [CreatedAt] datetimeoffset(7) NOT NULL,

        [AttemptNumber] int NOT NULL CONSTRAINT [DF_Outbox_AttemptNumber] DEFAULT (0),

        [AvailableAt] datetimeoffset(7) NOT NULL,
        [LeaseId] uniqueidentifier NULL,
        [LeaseExpiresAt] datetimeoffset(7) NULL,

        [ProcessedAt] datetimeoffset(7) NULL,
        [FailedAt] datetimeoffset(7) NULL,
        [LastError] nvarchar(4000) NULL,

        CONSTRAINT [PK_Outbox] PRIMARY KEY CLUSTERED ([MessageId]),

        CONSTRAINT [CK_Outbox_AttemptNumber] CHECK ([AttemptNumber] >= 0),

        CONSTRAINT [CK_Outbox_Lease]
            CHECK
            (
                ([LeaseId] IS NULL AND [LeaseExpiresAt] IS NULL)
                OR
                ([LeaseId] IS NOT NULL AND [LeaseExpiresAt] IS NOT NULL)
            ),

        CONSTRAINT [CK_Outbox_FinalState]
            CHECK
            (
                [ProcessedAt] IS NULL OR [FailedAt] IS NULL
            )
    );
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_Outbox_Claim' AND [object_id] = OBJECT_ID(N'[asyncomnia].[Outbox]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Outbox_Claim]
        ON [asyncomnia].[Outbox]
        (
            [AvailableAt],
            [CreatedAt],
            [MessageId]
        )
        INCLUDE
        (
            [LeaseExpiresAt],
            [AttemptNumber]
        )
        WHERE [ProcessedAt] IS NULL AND [FailedAt] IS NULL;
END;