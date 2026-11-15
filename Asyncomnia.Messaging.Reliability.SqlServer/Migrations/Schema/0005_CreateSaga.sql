IF OBJECT_ID(N'[asyncomnia].[Saga]', N'U') IS NULL
BEGIN
    CREATE TABLE [asyncomnia].[Saga]
    (
        [SagaType] nvarchar(1000) NOT NULL,
        [CorrelationId] uniqueidentifier NOT NULL,
        [CurrentState] nvarchar(255) NOT NULL,

        [DataJson] nvarchar(max) NOT NULL,
        [CreatedAt] datetimeoffset(7) NOT NULL CONSTRAINT [DF_Saga_CreatedAt] DEFAULT (SYSUTCDATETIME()),
        [UpdatedAt] datetimeoffset(7) NOT NULL CONSTRAINT [DF_Saga_UpdatedAt]  DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT [PK_Saga] PRIMARY KEY CLUSTERED
        (
            [SagaType],
            [CorrelationId]
        ),

        CONSTRAINT [CK_Saga_CurrentState] CHECK (LEN(LTRIM(RTRIM([CurrentState]))) > 0),
        CONSTRAINT [CK_Saga_DataJson] CHECK (ISJSON([DataJson]) = 1)
    );
END;