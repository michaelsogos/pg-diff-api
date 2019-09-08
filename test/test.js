const PgDiff = require("../src/index").PgDiff;
const Config = require("../src/index").Config;

Config.migrationHistoryTableName = "test_migrations";
Config.targetClient.database = "pgdiff_test";
Config.targetClient.password = "f";
Config.patchesFolder = ".\\test\\patches";

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
