IF SCHEMA_ID(N'asyncomnia') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA [asyncomnia]');
END;

IF OBJECT_ID(N'[asyncomnia].[SchemaMigrations]', N'U') IS NULL
BEGIN
    CREATE TABLE [asyncomnia].[SchemaMigrations]
    (
        [Component] nvarchar(200) NOT NULL,
        [MigrationId] nvarchar(200) NOT NULL,
        [AppliedAt] datetimeoffset(7) NOT NULL,

        CONSTRAINT [PK_SchemaMigrations] PRIMARY KEY CLUSTERED
        (
            [Component],
            [MigrationId]
        )
    );
END;
