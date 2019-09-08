const core = require("../core");

class CompareApi {
    static async migrate(config) {
        let pgClient = await core.makePgClient(config.targetClient);


    }
}

module.exports = CompareApi;
