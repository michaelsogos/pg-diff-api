const path = require("path");
const pg = require("pg");
const migrationHistoryTableSchema = require("./models/migrationHistoryTableSchema");
const sql = require("./sqlScriptGenerator");
const PatchInfo = require("./models/patchInfo");
const ServerVersion = require("./models/serverVersion");

class core {
    /**
     *
     * @param {import("./models/config")} config
     */
    static prepareMigrationConfig(config) {
        if (!config.patchesFolder) throw new Error('Missing configuration property "patchesFolder"!');
        return {
            patchesFolder: path.isAbsolute(config.patchesFolder) ? config.patchesFolder : path.resolve(process.cwd(), config.patchesFolder),
            migrationHistory: {
                tableName: config.migrationOptions.historyTableName,
                tableSchema: config.migrationOptions.historyTableSchema,
                fullTableName: `"${config.migrationOptions.historyTableSchema}"."${config.migrationOptions.historyTableName}"`,
                primaryKeyName: `"${config.migrationOptions.historyTableName}_pkey"`,
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

        client.version = await this.getServerVersion(client);

        return client;
    }

    /**
     *
     * @param {import("pg").Client} client
     */
    static async getServerVersion(client) {
        let version = null;
        let queryResult = await client.query("SELECT current_setting('server_version')");
        if (queryResult && queryResult.rows.length == 1 && queryResult.rows[0].current_setting) version = queryResult.rows[0].current_setting;

        if (typeof version != "string") return null;

        var splittedVersion = version.split(".");

        return new ServerVersion(parseInt(splittedVersion[0]), parseInt(splittedVersion[1]), parseInt(splittedVersion[2]), version);
    }

    /**
     *
     * @param {import("./models/serverVersion")} serverVersion
     * @param {Number} majorVersion
     * @param {Number} minorVersion
     * @returns {Boolean}
     */
    static checkServerCompatibility(serverVersion, majorVersion, minorVersion) {
        if (serverVersion != null && serverVersion.major >= majorVersion && client.version.minor >= minorVersion) return true;
        else return false;
    }
}

module.exports = core;
