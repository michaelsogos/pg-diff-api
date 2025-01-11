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
		if (!config.migrationOptions.patchesDirectory) throw new Error('Missing configuration property "patchesFolder"!');

		return {
			patchesFolder: path.isAbsolute(config.migrationOptions.patchesDirectory)
				? config.migrationOptions.patchesDirectory
				: path.resolve(process.cwd(), config.migrationOptions.patchesDirectory),
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

		let sqlScript = sql.generateCreateTableScript(config.migrationHistory.tableName, migrationHistoryTableSchema, config);
		await pgClient.query(sqlScript);
	}

	/**
	 *
	 * @param {String} filename
	 * @param {String} filepath
	 * @returns {import("./models/patchInfo")}
	 */
	static getPatchFileInfo(filename, filepath) {
		let indexOfSeparator = filename.indexOf("_");
		let version = filename.substring(0, indexOfSeparator);
		let name = filename.substring(indexOfSeparator + 1).replace(".sql", "");

		if (indexOfSeparator < 0 || !/^\d+$/.test(version))
			throw new Error(`The patch file name ${filename} is not compatible with conventioned pattern {version}_{path name}.sql !`);

		return new PatchInfo(filename, filepath, version, name);
	}

	/**
	 *
	 * @param {import("./models/clientConfig")} config
	 * @returns {import("pg").Client} Return a connected client
	 */
	static async makePgClient(config) {
		if (!config.database) throw new Error(`The client config parameter [database] cannot be empty! `);

		let client = new pg.Client({
			user: config.user,
			host: config.host,
			database: config.database,
			password: config.password,
			port: config.port,
			application_name: config.applicationName,
			ssl: config.ssl,
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
	 * Check the server version
	 * @param {import("./models/serverVersion")} serverVersion
	 * @param {Number} majorVersion
	 * @param {Number} minorVersion
	 * @returns {Boolean} Return true if connected server has greater or equal version
	 */
	static checkServerCompatibility(serverVersion, majorVersion, minorVersion) {
		if (serverVersion != null && serverVersion.major >= majorVersion && serverVersion.minor >= minorVersion) return true;
		else return false;
	}

	/**
	 * Retrive GIT CONFIG for USER NAME and USER EMAIL, repository first or fallback to global config
	 * @returns {String}
	 */
	static async getGitAuthor() {
		const util = require("util");
		const exec = util.promisify(require("child_process").exec);
		// eslint-disable-next-line no-unused-vars
		const childProcess = require("child_process");

		async function getLocalAuthorName() {
			let result = null;

			try {
				// eslint-disable-next-line no-unused-vars
				const { stdout, stderr } = await exec("git config --local user.name");
				result = stdout.trim();
			} catch (err) {
				result = err.stdout.trim();
			}

			return result;
		}

		async function getLocalAuthorEmail() {
			let result = null;

			try {
				// eslint-disable-next-line no-unused-vars
				const { stdout, stderr } = await exec("git config --local user.email");
				result = stdout.trim();
			} catch (err) {
				result = err.stdout.trim();
			}

			return result;
		}

		async function getGlobalAuthorName() {
			let result = null;

			try {
				// eslint-disable-next-line no-unused-vars
				const { stdout, stderr } = await exec("git config --global user.name");
				result = stdout.trim();
			} catch (err) {
				result = err.stdout.trim();
			}

			return result;
		}

		async function getGlobalAuthorEmail() {
			let result = null;

			try {
				// eslint-disable-next-line no-unused-vars
				const { stdout, stderr } = await exec("git config --global user.email");
				result = stdout.trim();
			} catch (err) {
				result = err.stdout.trim();
			}

			return result;
		}

		async function getDefaultAuthorName() {
			let result = null;

			try {
				// eslint-disable-next-line no-unused-vars
				const { stdout, stderr } = await exec("git config user.name");
				result = stdout.trim();
			} catch (err) {
				result = err.stdout.trim();
			}

			return result;
		}

		async function getDefaultAuthorEmail() {
			let result = null;

			try {
				// eslint-disable-next-line no-unused-vars
				const { stdout, stderr } = await exec("git config user.email");
				result = stdout.trim();
			} catch (err) {
				result = err.stdout.trim();
			}

			return result;
		}

		let authorName = await getLocalAuthorName();
		let authorEmail = await getLocalAuthorEmail();

		if (!authorName) {
			//GIT LOCAL didn't return anything! Try GIT GLOBAL.

			authorName = await getGlobalAuthorName();
			authorEmail = await getGlobalAuthorEmail();

			if (!authorName) {
				//Also GIT GLOBAL didn't return anything! Try GIT defaults.

				authorName = await getDefaultAuthorName();
				authorEmail = await getDefaultAuthorEmail();
			}
		}

		if (!authorName) return "Unknown author configured on this Git Repository";
		else if (authorEmail) return `${authorName} (${authorEmail})`;
		else return authorName;
	}
}

module.exports = core;
