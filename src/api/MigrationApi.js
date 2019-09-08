const fs = require("fs");
const path = require("path");
const sql = require("../sqlScriptGenerator");
const core = require("../core");
const patchStatus = require("../enums/patchStatus");
const textReader = require("line-by-line");

class MigrationApi {
    static async migrate(config, force) {
        let migrationConfig = core.prepareMigrationConfig(config);
        let pgClient = await core.makePgClient(config.targetClient);

        await core.prepareMigrationsHistoryTable(pgClient, migrationConfig);

        let patchesFiles = fs
            .readdirSync(migrationConfig.patchesFolder)
            .sort()
            .filter(file => {
                return file.match(/.*\.(sql)/gi);
            });

        let result = [];

        for (let index in patchesFiles) {
            let patchFileInfo = core.getPatchFileInfo(patchesFiles[index], migrationConfig.patchesFolder);
            let patchFileStatus = await this.checkPatchStatus(pgClient, patchFileInfo, migrationConfig);

            switch (patchFileStatus) {
                case patchStatus.IN_PROGRESS:
                    {
                        if (!force)
                            throw new Error(`The patch version={${patchFileInfo.version}} and name={${patchFileInfo.name}} is still in progress!`);

                        await this.applyPatch(pgClient, patchFileInfo, migrationConfig);
                        result.push(patchFileInfo);
                    }
                    break;
                case patchStatus.ERROR:
                    {
                        if (!force)
                            throw new Error(`The patch version={${patchFileInfo.version}} and name={${patchFileInfo.name}} encountered an error!`);

                        await this.applyPatch(pgClient, patchFileInfo, migrationConfig);
                        result.push(patchFileInfo);
                    }
                    break;
                case patchStatus.DONE:
                    break;
                case patchStatus.TO_APPLY:
                    await this.applyPatch(pgClient, patchFileInfo, migrationConfig);
                    result.push(patchFileInfo);
                    break;
                default:
                    throw new Error(
                        `The status "${patchFileStatus}" not recognized! Impossible to apply patch version={${patchFileInfo.version}} and name={${patchFileInfo.name}}.`,
                    );
            }
        }

        return result;
    }

    static async savePatch(config, patchFileName) {
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

    static async checkPatchStatus(pgClient, patchFileInfo, config) {
        let sql = `SELECT "status" FROM ${config.migrationHistory.fullTableName} WHERE "version" = '${patchFileInfo.version}' AND "name" = '${patchFileInfo.name}'`;
        let response = await pgClient.query(sql);

        if (response.rows.length > 1)
            throw new Error(
                `Too many patches found on migrations history table "${config.migrationHistory.fullTableName}" for patch version=${patchFileInfo.version} and name=${patchFileInfo.name}!`,
            );

        if (response.rows.length < 1) return patchStatus.TO_APPLY;
        else return response.rows[0].status;
    }

    static async applyPatch(pgClient, patchFileInfo, config) {
        var self = this;
        return new Promise(async (resolve, reject) => {
            try {
                await this.addRecordToHistoryTable(pgClient, patchFileInfo, config);

                let readingBlock = false;
                let readLines = 0;
                let commandExecuted = 0;
                let patchError = null;
                let patchScript = patchFileInfo;
                patchScript.command = "";
                patchScript.message = "";

                let reader = new textReader(path.resolve(patchFileInfo.filepath, patchFileInfo.filename));

                reader.on("error", err => {
                    reject(err);
                });

                reader.on("line", function(line) {
                    readLines += 1;
                    if (readingBlock) {
                        if (line.startsWith("--- END")) {
                            readingBlock = false;
                            reader.pause();
                            self.executePatchScript(pgClient, patchScript, config)
                                .then(() => {
                                    commandExecuted += 1;
                                    reader.resume();
                                })
                                .catch(err => {
                                    commandExecuted += 1;
                                    patchError = err;
                                    reader.close();
                                    reader.resume();
                                });
                        } else {
                            patchScript.command += `${line}\n`;
                        }
                    }

                    if (!readingBlock && line.startsWith("--- BEGIN")) {
                        readingBlock = true;
                        patchScript.command = "";
                        patchScript.message = line;
                    }
                });

                reader.on("end", async function() {
                    if (readLines <= 0) patchError = new Error(`The patch "${patchFileInfo.name}" version "${patchFileInfo.version}" is empty!`);
                    else if (commandExecuted <= 0)
                        patchError = new Error(
                            `The patch "${patchFileInfo.name}" version "${patchFileInfo.version}" is malformed. Missing BEGIN/END comments!`,
                        );

                    if (patchError) {
                        patchScript.status = patchStatus.ERROR;
                        patchScript.message = patchError.toString();
                    } else {
                        patchScript.status = patchStatus.DONE;
                        patchScript.message = "";
                    }

                    await self.updateRecordToHistoryTable(pgClient, patchScript, config);

                    if (patchError) reject(patchError);
                    else resolve();
                });
            } catch (e) {
                reject(e);
            }
        });
    }

    static async executePatchScript(pgClient, patchScript, config) {
        patchScript.status = patchStatus.IN_PROGRESS;
        await this.updateRecordToHistoryTable(pgClient, patchScript, config);
        await pgClient.query(patchScript.command);
    }

    static async updateRecordToHistoryTable(pgClient, patchScript, config) {
        let changes = {
            status: patchScript.status,
            last_message: patchScript.message,
            script: patchScript.command,
            applied_on: new Date(),
        };

        let filterConditions = {
            version: patchScript.version,
            name: patchScript.name
        };

        let command = sql.generateUpdateTableRecordScript(
            config.migrationHistory.fullTableName,
            config.migrationHistory.tableColumns,
            filterConditions,
            changes,
        );

        await pgClient.query(command);
    }

    static async addRecordToHistoryTable(pgClient, patchFileInfo, config) {
        let changes = {
            version: patchFileInfo.version,
            name: patchFileInfo.name,
            status: patchStatus.TO_APPLY,
            last_message: "",
            script: "",
            applied_on: null,
        };

        let options = {
            constraintName: config.migrationHistory.primaryKeyName,
        };

        let command = sql.generateMergeTableRecord(config.migrationHistory.fullTableName, config.migrationHistory.tableColumns, changes, options);
        await pgClient.query(command);
    }
}

module.exports = MigrationApi;
