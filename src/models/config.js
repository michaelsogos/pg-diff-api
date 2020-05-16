module.exports = {
	targetClient: {
		host: "localhost",
		port: 5432,
		/** @type {String} */
		database: null,
		user: "postgres",
		/** @type {String} */
		/** @type {String} */
		password: null,
		applicationName: "pg-diff-api",
	},
	sourceClient: {
		host: "localhost",
		port: 5432,
		database: null,
		user: "postgres",
		/** @type {String} */
		password: null,
		applicationName: "pg-diff-api",
	},
	compareOptions: {
		outputDirectory: "db_patches",
		/** @type {String} */
		author: null,
		getAuthorFromGit: true,
		schemaCompare: {
			namespaces: ["public"],
			dropMissingTable: false,
			dropMissingView: false,
			dropMissingFunction: false,
			dropMissingAggregate: false,
			/** @type {String[]} */
			roles: [],
		},
		dataCompare: {
			enable: true,
			/** @type {import("./tableDefinition")[]} */
			tables: [],
		},
	},
	migrationOptions: {
		/** @type {String} */
		patchesDirectory: null,
		historyTableName: "migrations",
		historyTableSchema: "public",
	},
};
