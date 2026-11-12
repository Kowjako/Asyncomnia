IF OBJECT_ID(N'[asyncomnia].[Inbox]', N'U') IS NULL
BEGIN
    CREATE TABLE [asyncomnia].[Inbox]
    (
        [MessageId] uniqueidentifier NOT NULL,
        [ConsumerId] nvarchar(255) NOT NULL,
        [ContractType] nvarchar(1000) NOT NULL,
        [ReceivedAt] datetimeoffset(7) NOT NULL,

        /* 
        Key with ConsumerId, so same MessageId can be handled by different subscription
        (MessageId X, billing-service) - should handle
        (MessageId X, auditing-service) - should handle
        */
        CONSTRAINT [PK_Inbox] PRIMARY KEY CLUSTERED
        (
            [ConsumerId],
            [MessageId]
        )
    );
END;