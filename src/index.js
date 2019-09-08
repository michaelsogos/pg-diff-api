const migrationApi = require("./api/MigrationApi");
const compareApi = require("./api/CompareApi");

class PgDiff {
    constructor(config) {
        this["config"] = config;
    }

    async migrate(force) {
        return await migrationApi.migrate(this.config, force);
    }

    async compare(){
        return await compareApi.compare();
    }
}

module.exports.PgDiff = PgDiff;
module.exports.Config = require("./models/config");
