const PgDiff = require("../src/index").PgDiff;
const Config = require("../src/index").Config;


Config.targetClient.database = "pg_diff_test2";
Config.sourceClient.database = "pg_diff_test1";
Config.sourceClient.port = 5437;
Config.targetClient.port = 5437;
Config.targetClient.password = "postgres";
Config.sourceClient.password = "postgres";

Config.migrationOptions.historyTableName = "test_migrations";
Config.migrationOptions.patchesDirectory = ".\\test\\patches";

var pgDiff = new PgDiff(Config);
pgDiff
    .migrate(false)
    .then(result => {
        console.log(`A total of ${result.length} patches have been applied.`);
        result.forEach((patch, index) => {
            console.log(`The patch "${patch.name}" version "${patch.version}" has been applied.`);
        });
    })
    .catch(err => {
        console.error(err);
    })
    .finally(() => {
        process.exit(0);
    });
