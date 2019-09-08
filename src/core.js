const path = require("path");
const pg = require("pg");
const migrationHistoryTableSchema = require("./models/migrationHistoryTableSchema");
const sql = require("./sqlScriptGenerator");
const PatchInfo = require("./models/patchInfo");

class core {
    static prepareMigrationConfig(config) {
        if (!config.patchesFolder) throw new Error('Missing configuration property "patchesFolder"!');
        return {
            patchesFolder: path.isAbsolute(config.patchesFolder) ? config.patchesFolder : path.resolve(process.cwd(), config.patchesFolder),
            migrationHistory: {
                tableName: config.migrationHistoryTableName,
                tableSchema: config.migrationHistoryTableSchema,
                fullTableName: `"${config.migrationHistoryTableSchema}"."${config.migrationHistoryTableName}"`,
                primaryKeyName: `"${config.migrationHistoryTableName}_pkey"`,
                tableOwner: config.targetClient.user,
                tableColumns: this.extractColumnsDefinitionFromSchema(migrationHistoryTableSchema),
            },
        };
    }

    static extractColumnsDefinitionFromSchema(schema) {
        let fields = [];
        for (let column in schema.columns) {
            fields.push({
                name: column,
                dataTypeCategory: schema.columns[column].dataTypeCategory,
            });
        }
        return fields;
    }

    static async prepareMigrationsHistoryTable(pgClient, config) {
        migrationHistoryTableSchema.constraints[config.migrationHistory.primaryKeyName] = {
            type: "p",
            definition: 'PRIMARY KEY ("version")',
        };

        migrationHistoryTableSchema.privileges[config.migrationHistory.tableOwner] = {
            select: true,
            insert: true,
            update: true,
            delete: true,
            truncate: true,
            references: true,
            trigger: true,
        };

        migrationHistoryTableSchema.owner = config.migrationHistory.tableOwner;

        let sqlScript = sql.generateCreateTableScript(config.migrationHistory.tableName, migrationHistoryTableSchema);
        await pgClient.query(sqlScript);
    }

    static getPatchFileInfo(filename, filepath) {
        let indexOfSeparator = filename.indexOf("_");
        let version = filename.substring(0, indexOfSeparator);
        let name = filename.substring(indexOfSeparator + 1).replace(".sql", "");

        if (indexOfSeparator < 0 || !/^\d+$/.test(version))
            throw new Error(`The patch file name ${filename} is not compatible with conventioned pattern {version}_{path name}.sql !`);

        return new PatchInfo(filename, filepath, version, name);
    }

    static async makePgClient(config) {
        let client = new pg.Client({
            user: config.user,
            host: config.host,
            database: config.database,
            password: config.password,
            port: config.port,
            application_name: config.applicationName,
        });
        await client.connect();
        return client;
    }
}

module.exports = core;
