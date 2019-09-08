module.exports = {
    migrationHistoryTableName: "migrations",
    migrationHistoryTableSchema: "public",
    patchesFolder: null,
    targetClient: {
        host: "localhost",
        port: 5432,
        database: null,
        user: "postgres",
        password: null,
        applicationName: "pg-diff-api",
    },
};
