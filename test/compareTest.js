const PgDiff = require("../src/index").PgDiff;
const Config = require("../src/index").Config;
const TableDefinition = require("../src/models/tableDefinition");

Config.targetClient.database = "pgdiff_test";
Config.sourceClient.database = "pgdiff_test_dev";
Config.compareOptions.outputDirectory = ".\\test\\patches";
Config.compareOptions.schemaCompare.roles = ["postgres", "huko"];
Config.compareOptions.dataCompare.enable = true;
Config.compareOptions.dataCompare.tables.push(new TableDefinition("test-api", ["id"]));
Config.compareOptions.dataCompare.tables.push(new TableDefinition("device", ["id"]));
Config.compareOptions.dataCompare.tables.push(new TableDefinition("special_fields", ["id"]));

var pgDiff = new PgDiff(Config);
pgDiff.events.on("compare", (message, percentage) => {
    console.log(`Complete at ${percentage}%: ${message}`);
});
pgDiff
    .compare("test_api_2")
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
