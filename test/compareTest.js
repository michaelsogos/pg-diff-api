const PgDiff = require("../src/index").PgDiff;
const Config = require("../src/index").Config;

Config.targetClient.database = "pgdiff_test";
Config.sourceClient.database = "pgdiff_test_dev";
Config.compareOptions.outputDirectory = ".\\test\\patches";
Config.compareOptions.schemaCompare.roles = ["postgres", "huko"];

var pgDiff = new PgDiff(Config);
pgDiff.events.on("compare", (message, percentage) => {
    console.log(`Complete at ${percentage}%: ${message}`);
});
pgDiff
    .compare("test_api")
    .then(result => {
        if (result == null) console.log("No patch has been created because no differences have been found!");
        else console.log(`The patch "${result}" has been created.`);
    })
    .catch(err => {
        console.error(`ERROR: ${err.message}\r\n${err.stack}`);
    })
    .finally(() => {
        process.exit(0);
    });
