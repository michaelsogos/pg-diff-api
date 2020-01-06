const migrationApi = require("./api/MigrationApi");
const compareApi = require("./api/CompareApi");
const EventEmitter = require("events");

class PgDiff {
    constructor(config) {
        this["config"] = config;
        /** @type {import("events")} */

        this.events = new EventEmitter();
    }

    /**
     *
     * @param {Boolean} force
     * @returns {Promise<import("./models/patchInfo")[]>} Return a list of PatchInfo.
     */
    async migrate(force) {
        force = force || false;
        return await migrationApi.migrate(this.config, force);
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

    /*
     async savePatch(config, patchFileName) {
        let migrationConfig = core.prepareMigrationConfig(config);
        let pgClient = await core.makePgClient(config.targetClient);

        await core.prepareMigrationsHistoryTable(pgClient, migrationConfig);

        let patchFilePath = path.resolve(migrationConfig.patchesFolder, patchFileName);

        if (!fs.existsSync(patchFilePath)) throw new Error(`The patch file ${patchFilePath} does not exists!`);

        let patchFileInfo = core.getPatchFileInfo(patchFileName, migrationConfig.patchesFolder);
        await this.addRecordToHistoryTable(pgClient, patchFileInfo, migrationConfig);
        patchFileInfo.status = patchStatus.DONE;
        await this.updateRecordToHistoryTable(pgClient, patchFileInfo, migrationConfig);
    }
    */
}

module.exports.PgDiff = PgDiff;
module.exports.Config = require("./models/config");
