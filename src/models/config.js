module.exports = {
    patchesFolder: null,
    targetClient: {
        host: "localhost",
        port: 5432,
        database: null,
        user: "postgres",
        password: null,
        applicationName: "pg-diff-api",
    },
    sourceClient: {
        host: "localhost",
        port: 5432,
        database: null,
        user: "postgres",
        password: null,
        applicationName: "pg-diff-api",
    },
    compareOptions: {
        outputDirectory: "db_patches",
        schemaCompare: {
            namespaces: ["public"],
            dropMissingTable: false,
            dropMissingView: false,
            dropMissingFunction: false,
        },
        dataCompare: {
            enable: true,
            /** @type {import("./tableDefinition")[]} */
            tables: [],
        },
    },
    migrationOptions: {
        historyTableName: "migrations",
        historyTableSchema: "public",
    },
};
