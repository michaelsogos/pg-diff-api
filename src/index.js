const migrationApi = require("./api/MigrationApi");
const compareApi = require("./api/CompareApi");
const EventEmitter = require("events");

class PgDiff {
	/**
	 *
	 * @param {import("./models/config")} config
	 */
	constructor(config) {
		this["config"] = config;

		/** @type {import("events")} */
		this.events = new EventEmitter();
	}

	/**
	 *
	 * @param {Boolean} force True to force execution even for patches encountered an error
	 * @param {Boolean} toSourceClient True to execute patches on source client
	 * @returns {Promise<import("./models/patchInfo")[]>} Return a list of PatchInfo.
	 */
	async migrate(force, toSourceClient) {
		force = force || false;
		toSourceClient = toSourceClient || false;
		return await migrationApi.migrate(this.config, force, toSourceClient, this.events);
	}

	/**
	 *
	 * @param {String} scriptName
	 * @returns {String} Return null if no patch has been created.
	 */
	async compare(scriptName) {
		if (!scriptName) throw new Error("The script name must be specified!");
		return await compareApi.compare(this.config, scriptName, this.events);
	}

	/**
	 *
	 * @param {String} patchFileName
	 */
	async save(patchFileName) {
		if (!patchFileName) throw new Error("The patch file name must be specified!");
		return await migrationApi.savePatch(this.config, patchFileName);
	}
}

module.exports.PgDiff = PgDiff;
module.exports.Config = require("./models/config");
