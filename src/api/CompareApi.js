const core = require("../core");
const catalogApi = require("./CatalogApi");
const DatabaseObjects = require("../models/databaseObjects");
const sql = require("../sqlScriptGenerator");
const TableData = require("../models/tableData");
const deepEqual = require("deep-equal");
const fs = require("fs");
const path = require("path");
const objectType = require("../enums/objectType");

class CompareApi {
	/**
	 *
	 * @param {import("../models/config")} config
	 * @param {String} scriptName
	 * @param {import("events")} eventEmitter
	 * @returns {Promise<String>} Return the sql patch file pathh
	 */
	static async compare(config, scriptName, eventEmitter) {
		eventEmitter.emit("compare", "Compare started", 0);

		eventEmitter.emit("compare", "Connecting to source database ...", 10);
		let pgSourceClient = await core.makePgClient(config.sourceClient);
		eventEmitter.emit(
			"compare",
			`Connected to source PostgreSQL ${pgSourceClient.version.version} on [${config.sourceClient.host}:${config.sourceClient.port}/${config.sourceClient.database}] `,
			11
		);

		eventEmitter.emit("compare", "Connecting to target database ...", 20);
		let pgTargetClient = await core.makePgClient(config.targetClient);
		eventEmitter.emit(
			"compare",
			`Connected to target PostgreSQL ${pgTargetClient.version.version} on [${config.targetClient.host}:${config.targetClient.port}/${config.targetClient.database}] `,
			21
		);

		let dbSourceObjects = await this.collectSchemaObjects(pgSourceClient, config);
		eventEmitter.emit("compare", "Collected SOURCE objects", 30);
		let dbTargetObjects = await this.collectSchemaObjects(pgTargetClient, config);
		eventEmitter.emit("compare", "Collected TARGET objects", 40);

		let droppedConstraints = [];
		let droppedIndexes = [];
		let droppedViews = [];
		let addedColumns = {};
		let addedTables = [];

		let scripts = this.compareDatabaseObjects(
			dbSourceObjects,
			dbTargetObjects,
			droppedConstraints,
			droppedIndexes,
			droppedViews,
			addedColumns,
			addedTables,
			config,
			eventEmitter
		);

		//The progress step size is 20
		if (config.compareOptions.dataCompare.enable) {
			scripts.push(
				...(await this.compareTablesRecords(
					config,
					pgSourceClient,
					pgTargetClient,
					addedColumns,
					addedTables,
					dbSourceObjects,
					dbTargetObjects,
					eventEmitter
				))
			);
			eventEmitter.emit("compare", "Table records have been compared", 95);
		}

		let scriptFilePath = await this.saveSqlScript(scripts, config, scriptName, eventEmitter);

		eventEmitter.emit("compare", "Compare completed", 100);

		return scriptFilePath;
	}

	/**
	 *
	 * @param {import("pg").Client} client
	 * @param {import("../models/config")} config
	 * @returns {Promise<import("../models/databaseObjects")>}
	 */
	static async collectSchemaObjects(client, config) {
		var dbObjects = new DatabaseObjects();

		if (typeof config.compareOptions.schemaCompare.namespaces === "string" || config.compareOptions.schemaCompare.namespaces instanceof String)
			config.compareOptions.schemaCompare.namespaces = [config.compareOptions.schemaCompare.namespaces];
		else if (
			!config.compareOptions.schemaCompare.namespaces ||
			!Array.isArray(config.compareOptions.schemaCompare.namespaces) ||
			config.compareOptions.schemaCompare.namespaces.length <= 0
		)
			config.compareOptions.schemaCompare.namespaces = await catalogApi.retrieveAllSchemas(client);

		dbObjects.schemas = await catalogApi.retrieveSchemas(client, config.compareOptions.schemaCompare.namespaces);
		dbObjects.tables = await catalogApi.retrieveTables(client, config);
		dbObjects.views = await catalogApi.retrieveViews(client, config);
		dbObjects.materializedViews = await catalogApi.retrieveMaterializedViews(client, config);
		dbObjects.functions = await catalogApi.retrieveFunctions(client, config);
		dbObjects.aggregates = await catalogApi.retrieveAggregates(client, config);
		dbObjects.sequences = await catalogApi.retrieveSequences(client, config);

		//TODO: Add a way to retrieve AGGREGATE and WINDOW functions
		//TODO: Do we need to retrieve roles?
		//TODO: Do we need to retieve special table like TEMPORARY and UNLOGGED? for sure not temporary, but UNLOGGED probably yes.
		//TODO: Do we need to retrieve collation for both table and columns?
		//TODO: Add a way to retrieve DOMAIN and its CONSTRAINTS

		return dbObjects;
	}

	/**
	 *
	 * @param {import("../models/databaseObjects")} dbSourceObjects
	 * @param {import("../models/databaseObjects")} dbTargetObjects
	 * @param {String[]} droppedConstraints
	 * @param {String[]} droppedIndexes
	 * @param {String[]} droppedViews
	 * @param {Object} addedColumns
	 * @param {String[]} addedTables
	 * @param {import("../models/config")} config
	 * @param {import("events")} eventEmitter
	 */
	static compareDatabaseObjects(
		dbSourceObjects,
		dbTargetObjects,
		droppedConstraints,
		droppedIndexes,
		droppedViews,
		addedColumns,
		addedTables,
		config,
		eventEmitter
	) {
		let sqlPatch = [];

		sqlPatch.push(...this.compareSchemas(dbSourceObjects.schemas, dbTargetObjects.schemas));
		eventEmitter.emit("compare", "SCHEMA objects have been compared", 45);

		sqlPatch.push(...this.compareSequences(dbSourceObjects.sequences, dbTargetObjects.sequences));
		eventEmitter.emit("compare", "SEQUENCE objects have been compared", 50);

		sqlPatch.push(
			...this.compareTables(
				dbSourceObjects.tables,
				dbTargetObjects,
				droppedConstraints,
				droppedIndexes,
				droppedViews,
				addedColumns,
				addedTables,
				config
			)
		);
		eventEmitter.emit("compare", "TABLE objects have been compared", 55);

		sqlPatch.push(...this.compareViews(dbSourceObjects.views, dbTargetObjects.views, droppedViews, config));
		eventEmitter.emit("compare", "VIEW objects have been compared", 60);

		sqlPatch.push(
			...this.compareMaterializedViews(
				dbSourceObjects.materializedViews,
				dbTargetObjects.materializedViews,
				droppedViews,
				droppedIndexes,
				config
			)
		);
		eventEmitter.emit("compare", "MATERIALIZED VIEW objects have been compared", 65);

		sqlPatch.push(...this.compareProcedures(dbSourceObjects.functions, dbTargetObjects.functions, config));
		eventEmitter.emit("compare", "PROCEDURE objects have been compared", 70);

		sqlPatch.push(...this.compareAggregates(dbSourceObjects.aggregates, dbTargetObjects.aggregates, config));
		eventEmitter.emit("compare", "AGGREGATE objects have been compared", 75);

		sqlPatch.push(...this.compareTablesTriggers(dbSourceObjects.tables, dbTargetObjects.tables, addedTables));
		eventEmitter.emit("compare", "TRIGGER objects have been compared", 80);

		return sqlPatch;
	}

	/**
	 *
	 * @param {String} scriptLabel
	 * @param {String[]} sqlScript
	 */
	static finalizeScript(scriptLabel, sqlScript) {
		let finalizedScript = [];

		if (sqlScript.length > 0) {
			finalizedScript.push(`\n--- BEGIN ${scriptLabel} ---\n`);
			finalizedScript.push(...sqlScript);
			finalizedScript.push(`\n--- END ${scriptLabel} ---\n`);
		}

		return finalizedScript;
	}

	/**
	 *
	 * @param {Object} sourceSchemas
	 * @param {Object} targetSchemas
	 */
	static compareSchemas(sourceSchemas, targetSchemas) {
		let finalizedScript = [];

		for (let sourceSchema in sourceSchemas) {
			let sqlScript = [];

			if (!targetSchemas[sourceSchema]) {
				//Schema not exists on target database, then generate script to create schema
				sqlScript.push(sql.generateCreateSchemaScript(sourceSchema, sourceSchemas[sourceSchema].owner));
				sqlScript.push(sql.generateChangeCommentScript(objectType.SCHEMA, sourceSchema, sourceSchemas[sourceSchema].comment));
			}

			if (targetSchemas[sourceSchema] && sourceSchemas[sourceSchema].comment != targetSchemas[sourceSchema].comment)
				sqlScript.push(sql.generateChangeCommentScript(objectType.SCHEMA, sourceSchema, sourceSchemas[sourceSchema].comment));

			finalizedScript.push(...this.finalizeScript(`CREATE OR UPDATE SCHEMA ${sourceSchema}`, sqlScript));
		}

		return finalizedScript;
	}

	/**
	 *
	 * @param {Object} sourceTables
	 * @param {import("../models/databaseObjects")} dbTargetObjects
	 * @param {String[]} droppedConstraints
	 * @param {String[]} droppedIndexes
	 * @param {String[]} droppedViews
	 * @param {Object} addedColumns
	 * @param {String[]} addedTables
	 * @param {import("../models/config")} config
	 */
	static compareTables(sourceTables, dbTargetObjects, droppedConstraints, droppedIndexes, droppedViews, addedColumns, addedTables, config) {
		let finalizedScript = [];

		for (let sourceTable in sourceTables) {
			let sqlScript = [];
			let actionLabel = "";

			if (dbTargetObjects.tables[sourceTable]) {
				//Table exists on both database, then compare table schema
				actionLabel = "ALTER";

				//@mso -> relhadoids has been deprecated from PG v12.0
				if (dbTargetObjects.tables[sourceTable].options)
					sqlScript.push(
						...this.compareTableOptions(sourceTable, sourceTables[sourceTable].options, dbTargetObjects.tables[sourceTable].options)
					);

				sqlScript.push(
					...this.compareTableColumns(
						sourceTable,
						sourceTables[sourceTable].columns,
						dbTargetObjects,
						droppedConstraints,
						droppedIndexes,
						droppedViews,
						addedColumns
					)
				);

				sqlScript.push(
					...this.compareTableConstraints(
						sourceTable,
						sourceTables[sourceTable].constraints,
						dbTargetObjects.tables[sourceTable].constraints,
						droppedConstraints
					)
				);

				sqlScript.push(
					...this.compareTableIndexes(sourceTables[sourceTable].indexes, dbTargetObjects.tables[sourceTable].indexes, droppedIndexes)
				);

				sqlScript.push(
					...this.compareTablePrivileges(
						sourceTable,
						sourceTables[sourceTable].privileges,
						dbTargetObjects.tables[sourceTable].privileges,
						config
					)
				);

				if (sourceTables[sourceTable].owner != dbTargetObjects.tables[sourceTable].owner)
					sqlScript.push(sql.generateChangeTableOwnerScript(sourceTable, sourceTables[sourceTable].owner));

				if (sourceTables[sourceTable].comment != dbTargetObjects.tables[sourceTable].comment)
					sqlScript.push(sql.generateChangeCommentScript(objectType.TABLE, sourceTable, sourceTables[sourceTable].comment));
			} else {
				//Table not exists on target database, then generate the script to create table
				actionLabel = "CREATE";
				addedTables.push(sourceTable);
				sqlScript.push(sql.generateCreateTableScript(sourceTable, sourceTables[sourceTable], config));
				sqlScript.push(sql.generateChangeCommentScript(objectType.TABLE, sourceTable, sourceTables[sourceTable].comment));
			}

			finalizedScript.push(...this.finalizeScript(`${actionLabel} TABLE ${sourceTable}`, sqlScript));
		}

		if (config.compareOptions.schemaCompare.dropMissingTable) {
			const migrationFullTableName = config.migrationOptions
				? `"${config.migrationOptions.historyTableSchema}"."${config.migrationOptions.historyTableName}"`
				: "";

			for (let table in dbTargetObjects.tables) {
				let sqlScript = [];

				if (!sourceTables[table] && table != migrationFullTableName) sqlScript.push(sql.generateDropTableScript(table));

				finalizedScript.push(...this.finalizeScript(`DROP TABLE ${table}`, sqlScript));
			}
		}

		return finalizedScript;
	}

	/**
	 *
	 * @param {String} tableName
	 * @param {Object} sourceTableOptions
	 * @param {Object} targetTableOptions
	 */
	static compareTableOptions(tableName, sourceTableOptions, targetTableOptions) {
		if (sourceTableOptions.withOids != targetTableOptions.withOids) return [sql.generateChangeTableOptionsScript(tableName, sourceTableOptions)];
		else return [];
	}

	/**
	 *
	 * @param {String} tableName
	 * @param {Array} sourceTableColumns
	 * @param {import("../models/databaseObjects")} dbTargetObjects
	 * @param {String[]} droppedConstraints
	 * @param {String[]} droppedIndexes
	 * @param {String[]} droppedViews
	 * @param {Object} addedColumns
	 */
	static compareTableColumns(tableName, sourceTableColumns, dbTargetObjects, droppedConstraints, droppedIndexes, droppedViews, addedColumns) {
		let sqlScript = [];
		let targetTable = dbTargetObjects.tables[tableName];

		for (let sourceTableColumn in sourceTableColumns) {
			if (targetTable.columns[sourceTableColumn]) {
				//Table column exists on both database, then compare column schema
				sqlScript.push(
					...this.compareTableColumn(
						tableName,
						sourceTableColumn,
						sourceTableColumns[sourceTableColumn],
						dbTargetObjects,
						droppedConstraints,
						droppedIndexes,
						droppedViews
					)
				);
			} else {
				//Table column not exists on target database, then generate script to add column
				sqlScript.push(sql.generateAddTableColumnScript(tableName, sourceTableColumn, sourceTableColumns[sourceTableColumn]));
				sqlScript.push(
					sql.generateChangeCommentScript(
						objectType.COLUMN,
						`${tableName}.${sourceTableColumn}`,
						sourceTableColumns[sourceTableColumn].comment
					)
				);

				if (!addedColumns[tableName]) addedColumns[tableName] = [];

				addedColumns[tableName].push(sourceTableColumn);
			}
		}

		for (let targetColumn in targetTable.columns) {
			if (!sourceTableColumns[targetColumn])
				//Table column not exists on source, then generate script to drop column
				sqlScript.push(sql.generateDropTableColumnScript(tableName, targetColumn));
		}

		return sqlScript;
	}

	/**
	 *
	 * @param {String} tableName
	 * @param {String} columnName
	 * @param {Object} sourceTableColumn
	 * @param {import("../models/databaseObjects")} dbTargetObjects
	 * @param {String[]} droppedConstraints
	 * @param {String[]} droppedIndexes
	 * @param {String[]} droppedViews
	 */
	static compareTableColumn(tableName, columnName, sourceTableColumn, dbTargetObjects, droppedConstraints, droppedIndexes, droppedViews) {
		let sqlScript = [];
		let changes = {};
		let targetTable = dbTargetObjects.tables[tableName];
		let targetTableColumn = targetTable.columns[columnName];

		if (sourceTableColumn.nullable != targetTableColumn.nullable) changes.nullable = sourceTableColumn.nullable;

		if (
			sourceTableColumn.datatype != targetTableColumn.datatype ||
			sourceTableColumn.precision != targetTableColumn.precision ||
			sourceTableColumn.scale != targetTableColumn.scale
		) {
			changes.datatype = sourceTableColumn.datatype;
			changes.dataTypeID = sourceTableColumn.dataTypeID;
			changes.dataTypeCategory = sourceTableColumn.dataTypeCategory;
			changes.precision = sourceTableColumn.precision;
			changes.scale = sourceTableColumn.scale;
		}

		if (sourceTableColumn.default != targetTableColumn.default) changes.default = sourceTableColumn.default;

		if (sourceTableColumn.identity != targetTableColumn.identity) {
			changes.identity = sourceTableColumn.identity;

			if (targetTableColumn.identity == null) changes.isNewIdentity = true;
			else changes.isNewIdentity = false;
		}

		if (
			sourceTableColumn.generatedColumn &&
			(sourceTableColumn.generatedColumn != targetTableColumn.generatedColumn || sourceTableColumn.default != targetTableColumn.default)
		) {
			changes = {};
			sqlScript.push(sql.generateDropTableColumnScript(tableName, columnName, true));
			sqlScript.push(sql.generateAddTableColumnScript(tableName, columnName, sourceTableColumn));
		}

		if (Object.keys(changes).length > 0) {
			let rawColumnName = columnName.substring(1).slice(0, -1);

			//Check if the column has constraint
			for (let constraint in targetTable.constraints) {
				if (droppedConstraints.includes(constraint)) continue;

				let constraintDefinition = targetTable.constraints[constraint].definition;
				let searchStartingIndex = constraintDefinition.indexOf("(");

				if (
					constraintDefinition.includes(`${rawColumnName},`, searchStartingIndex) ||
					constraintDefinition.includes(`${rawColumnName})`, searchStartingIndex) ||
					constraintDefinition.includes(`${columnName}`, searchStartingIndex)
				) {
					sqlScript.push(sql.generateDropTableConstraintScript(tableName, constraint));
					droppedConstraints.push(constraint);
				}
			}

			//Check if the column is part of indexes
			for (let index in targetTable.indexes) {
				let indexDefinition = targetTable.indexes[index].definition;
				let serachStartingIndex = indexDefinition.indexOf("(");

				if (
					indexDefinition.includes(`${rawColumnName},`, serachStartingIndex) ||
					indexDefinition.includes(`${rawColumnName})`, serachStartingIndex) ||
					indexDefinition.includes(`${columnName}`, serachStartingIndex)
				) {
					sqlScript.push(sql.generateDropIndexScript(index));
					droppedIndexes.push(index);
				}
			}

			//Check if the column is used into view
			for (let view in dbTargetObjects.views) {
				dbTargetObjects.views[view].dependencies.forEach((dependency) => {
					let fullDependencyName = `"${dependency.schemaName}"."${dependency.tableName}"`;
					if (fullDependencyName == tableName && dependency.columnName == columnName) {
						sqlScript.push(sql.generateDropViewScript(view));
						droppedViews.push(view);
					}
				});
			}

			//Check if the column is used into materialized view
			for (let view in dbTargetObjects.materializedViews) {
				dbTargetObjects.materializedViews[view].dependencies.forEach((dependency) => {
					let fullDependencyName = `"${dependency.schemaName}"."${dependency.tableName}"`;
					if (fullDependencyName == tableName && dependency.columnName == columnName) {
						sqlScript.push(sql.generateDropMaterializedViewScript(view));
						droppedViews.push(view);
					}
				});
			}

			sqlScript.push(sql.generateChangeTableColumnScript(tableName, columnName, changes));
		}

		if (sourceTableColumn.comment != targetTableColumn.comment)
			sqlScript.push(sql.generateChangeCommentScript(objectType.COLUMN, `${tableName}.${columnName}`, sourceTableColumn.comment));

		return sqlScript;
	}

	/**
	 *
	 * @param {String} tableName
	 * @param {Object} sourceTableConstraints
	 * @param {Object} targetTableConstraints
	 * @param {String[]} droppedConstraints
	 */
	static compareTableConstraints(tableName, sourceTableConstraints, targetTableConstraints, droppedConstraints) {
		let sqlScript = [];

		for (let constraint in sourceTableConstraints) {
			//Get new or changed constraint
			if (targetTableConstraints[constraint]) {
				//Table constraint exists on both database, then compare column schema
				if (sourceTableConstraints[constraint].definition != targetTableConstraints[constraint].definition) {
					if (!droppedConstraints.includes(constraint)) {
						sqlScript.push(sql.generateDropTableConstraintScript(tableName, constraint));
					}
					sqlScript.push(sql.generateAddTableConstraintScript(tableName, constraint, sourceTableConstraints[constraint]));
					sqlScript.push(
						sql.generateChangeCommentScript(objectType.CONSTRAINT, constraint, sourceTableConstraints[constraint].comment, tableName)
					);
				} else {
					if (droppedConstraints.includes(constraint)) {
						//It will recreate a dropped constraints because changes happens on involved columns
						sqlScript.push(sql.generateAddTableConstraintScript(tableName, constraint, sourceTableConstraints[constraint]));
						sqlScript.push(
							sql.generateChangeCommentScript(objectType.CONSTRAINT, constraint, sourceTableConstraints[constraint].comment, tableName)
						);
					} else {
						if (sourceTableConstraints[constraint].comment != targetTableConstraints[constraint].comment)
							sqlScript.push(
								sql.generateChangeCommentScript(
									objectType.CONSTRAINT,
									constraint,
									sourceTableConstraints[constraint].comment,
									tableName
								)
							);
					}
				}
			} else {
				//Table constraint not exists on target database, then generate script to add constraint
				sqlScript.push(sql.generateAddTableConstraintScript(tableName, constraint, sourceTableConstraints[constraint]));
				sqlScript.push(
					sql.generateChangeCommentScript(objectType.CONSTRAINT, constraint, sourceTableConstraints[constraint].comment, tableName)
				);
			}
		}

		for (let constraint in targetTableConstraints) {
			//Get dropped constraints
			if (!sourceTableConstraints[constraint] && !droppedConstraints.includes(constraint))
				//Table constraint not exists on source, then generate script to drop constraint
				sqlScript.push(sql.generateDropTableConstraintScript(tableName, constraint));
		}

		return sqlScript;
	}

	/**
	 *
	 * @param {Object} sourceTableIndexes
	 * @param {Object} targetTableIndexes
	 * @param {String[]} droppedIndexes
	 */
	static compareTableIndexes(sourceTableIndexes, targetTableIndexes, droppedIndexes) {
		let sqlScript = [];

		for (let index in sourceTableIndexes) {
			//Get new or changed indexes
			if (targetTableIndexes[index]) {
				//Table index exists on both database, then compare index definition
				if (sourceTableIndexes[index].definition != targetTableIndexes[index].definition) {
					if (!droppedIndexes.includes(index)) {
						sqlScript.push(sql.generateDropIndexScript(index));
					}
					sqlScript.push(`\n${sourceTableIndexes[index].definition};\n`);
					sqlScript.push(
						sql.generateChangeCommentScript(
							objectType.INDEX,
							`"${sourceTableIndexes[index].schema}"."${index}"`,
							sourceTableIndexes[index].comment
						)
					);
				} else {
					if (droppedIndexes.includes(index)) {
						//It will recreate a dropped index because changes happens on involved columns
						sqlScript.push(`\n${sourceTableIndexes[index].definition};\n`);
						sqlScript.push(
							sql.generateChangeCommentScript(
								objectType.INDEX,
								`"${sourceTableIndexes[index].schema}"."${index}"`,
								sourceTableIndexes[index].comment
							)
						);
					} else {
						if (sourceTableIndexes[index].comment != targetTableIndexes[index].comment)
							sqlScript.push(
								sql.generateChangeCommentScript(
									objectType.INDEX,
									`"${sourceTableIndexes[index].schema}"."${index}"`,
									sourceTableIndexes[index].comment
								)
							);
					}
				}
			} else {
				//Table index not exists on target database, then generate script to add index
				sqlScript.push(`\n${sourceTableIndexes[index].definition};\n`);
				sqlScript.push(
					sql.generateChangeCommentScript(
						objectType.INDEX,
						`"${sourceTableIndexes[index].schema}"."${index}"`,
						sourceTableIndexes[index].comment
					)
				);
			}
		}

		for (let index in targetTableIndexes) {
			//Get dropped indexes
			if (!sourceTableIndexes[index] && !droppedIndexes.includes(index))
				//Table index not exists on source, then generate script to drop index
				sqlScript.push(sql.generateDropIndexScript(index));
		}

		return sqlScript;
	}

	/**
	 *
	 * @param {String} tableName
	 * @param {Object} sourceTablePrivileges
	 * @param {Object} targetTablePrivileges
	 * @param {import("../models/config")} config
	 */
	static compareTablePrivileges(tableName, sourceTablePrivileges, targetTablePrivileges, config) {
		let sqlScript = [];

		for (let role in sourceTablePrivileges) {
			// In case a list of specific roles hve been configured, the check will only contains those roles eventually.
			if (config.compareOptions.schemaCompare.roles.length > 0 && !config.compareOptions.schemaCompare.roles.includes(role)) continue;

			//Get new or changed role privileges
			if (targetTablePrivileges[role]) {
				//Table privileges for role exists on both database, then compare privileges
				let changes = {};

				if (sourceTablePrivileges[role].select != targetTablePrivileges[role].select) changes.select = sourceTablePrivileges[role].select;

				if (sourceTablePrivileges[role].insert != targetTablePrivileges[role].insert) changes.insert = sourceTablePrivileges[role].insert;

				if (sourceTablePrivileges[role].update != targetTablePrivileges[role].update) changes.update = sourceTablePrivileges[role].update;

				if (sourceTablePrivileges[role].delete != targetTablePrivileges[role].delete) changes.delete = sourceTablePrivileges[role].delete;

				if (sourceTablePrivileges[role].truncate != targetTablePrivileges[role].truncate)
					changes.truncate = sourceTablePrivileges[role].truncate;

				if (sourceTablePrivileges[role].references != targetTablePrivileges[role].references)
					changes.references = sourceTablePrivileges[role].references;

				if (sourceTablePrivileges[role].trigger != targetTablePrivileges[role].trigger) changes.trigger = sourceTablePrivileges[role].trigger;

				if (Object.keys(changes).length > 0) sqlScript.push(sql.generateChangesTableRoleGrantsScript(tableName, role, changes));
			} else {
				//Table grants for role not exists on target database, then generate script to add role privileges
				sqlScript.push(sql.generateTableRoleGrantsScript(tableName, role, sourceTablePrivileges[role]));
			}
		}

		return sqlScript;
	}

	/**
	 *
	 * @param {Object} sourceTables
	 * @param {Object} targetTables
	 * @param {String[]} addedTables
	 * @returns
	 */
	static compareTablesTriggers(sourceTables, targetTables, addedTables) {
		let finalizedScript = [];

		for (let sourceTable in sourceTables) {
			let sqlScript = [];

			if (targetTables[sourceTable]) {
				//Table exists on both database, then compare trigger schema
				sqlScript.push(...this.compareTableTriggers(sourceTable, sourceTables[sourceTable].triggers, targetTables[sourceTable].triggers));
			}

			// triggers on newly added tatbles
			if (addedTables.includes(sourceTable)) sqlScript.push(...this.compareTableTriggers(sourceTable, sourceTables[sourceTable].triggers, {}));

			finalizedScript.push(...this.finalizeScript(`SET TRIGGERS FOR ${sourceTable}`, sqlScript));
		}

		return finalizedScript;
	}

	/**
	 *
	 * @param {String} tableName
	 * @param {Object} sourceTableTriggers
	 * @param {Object} targetTableTriggers
	 * @returns
	 */
	static compareTableTriggers(tableName, sourceTableTriggers, targetTableTriggers) {
		let sqlScript = [];
		// source triggers
		for (let trigger in sourceTableTriggers) {
			if (targetTableTriggers[trigger]) {
				//Trigger exists on both database, then compare trigger definition
				if (sourceTableTriggers[trigger].definition != targetTableTriggers[trigger].definition) {
					sqlScript.push(sql.generateDropTriggerScript(tableName, trigger));
					sqlScript.push(sql.generateCreateTriggerScript(sourceTableTriggers[trigger]));
					if (sourceTableTriggers[trigger].comment != targetTableTriggers[trigger].comment)
						sqlScript.push(sql.generateChangeCommentScript(objectType.TRIGGER, trigger, sourceTableTriggers[trigger].comment, tableName));
				}
			} else {
				//Trigger not exists on target database, then generate the script to create trigger
				sqlScript.push(sql.generateCreateTriggerScript(sourceTableTriggers[trigger]));
				sqlScript.push(sql.generateChangeCommentScript(objectType.TRIGGER, trigger, sourceTableTriggers[trigger].comment, tableName));
			}
		}
		// target triggers to be deleted
		for (let trigger in targetTableTriggers) {
			if (!sourceTableTriggers[trigger]) {
				sqlScript.push(sql.generateDropTriggerScript(tableName, trigger));
			}
		}
		return sqlScript;
	}

	/**
	 *
	 * @param {Object} sourceViews
	 * @param {Object} targetViews
	 * @param {String[]} droppedViews
	 * @param {import("../models/config")} config
	 */
	static compareViews(sourceViews, targetViews, droppedViews, config) {
		let finalizedScript = [];

		for (let view in sourceViews) {
			let sqlScript = [];
			let actionLabel = "";

			if (targetViews[view]) {
				//View exists on both database, then compare view schema
				actionLabel = "ALTER";

				let sourceViewDefinition = sourceViews[view].definition.replace(/\r/g);
				let targetViewDefinition = targetViews[view].definition.replace(/\r/g);
				if (sourceViewDefinition != targetViewDefinition) {
					if (!droppedViews.includes(view)) sqlScript.push(sql.generateDropViewScript(view));
					sqlScript.push(sql.generateCreateViewScript(view, sourceViews[view]));
					sqlScript.push(sql.generateChangeCommentScript(objectType.VIEW, view, sourceViews[view].comment));
				} else {
					if (droppedViews.includes(view))
						//It will recreate a dropped view because changes happens on involved columns
						sqlScript.push(sql.generateCreateViewScript(view, sourceViews[view]));

					sqlScript.push(...this.compareTablePrivileges(view, sourceViews[view].privileges, targetViews[view].privileges, config));

					if (sourceViews[view].owner != targetViews[view].owner)
						sqlScript.push(sql.generateChangeTableOwnerScript(view, sourceViews[view].owner));

					if (sourceViews[view].comment != targetViews[view].comment)
						sqlScript.push(sql.generateChangeCommentScript(objectType.VIEW, view, sourceViews[view].comment));
				}
			} else {
				//View not exists on target database, then generate the script to create view
				actionLabel = "CREATE";

				sqlScript.push(sql.generateCreateViewScript(view, sourceViews[view]));
				sqlScript.push(sql.generateChangeCommentScript(objectType.VIEW, view, sourceViews[view].comment));
			}

			finalizedScript.push(...this.finalizeScript(`${actionLabel} VIEW ${view}`, sqlScript));
		}

		if (config.compareOptions.schemaCompare.dropMissingView)
			for (let view in targetViews) {
				//Get missing views
				let sqlScript = [];

				if (!sourceViews[view]) sqlScript.push(sql.generateDropViewScript(view));

				finalizedScript.push(...this.finalizeScript(`DROP VIEW ${view}`, sqlScript));
			}

		return finalizedScript;
	}

	/**
	 *
	 * @param {Object} sourceMaterializedViews
	 * @param {Object} targetMaterializedViews
	 * @param {String[]} droppedViews
	 * @param {String[]} droppedIndexes
	 * @param {import("../models/config")} config
	 */
	static compareMaterializedViews(sourceMaterializedViews, targetMaterializedViews, droppedViews, droppedIndexes, config) {
		let finalizedScript = [];

		for (let view in sourceMaterializedViews) {
			//Get new or changed materialized views
			let sqlScript = [];
			let actionLabel = "";

			if (targetMaterializedViews[view]) {
				//Materialized view exists on both database, then compare materialized view schema
				actionLabel = "ALTER";

				let sourceViewDefinition = sourceMaterializedViews[view].definition.replace(/\r/g);
				let targetViewDefinition = targetMaterializedViews[view].definition.replace(/\r/g);
				if (sourceViewDefinition != targetViewDefinition) {
					if (!droppedViews.includes(view)) sqlScript.push(sql.generateDropMaterializedViewScript(view));
					sqlScript.push(sql.generateCreateMaterializedViewScript(view, sourceMaterializedViews[view]));
					sqlScript.push(sql.generateChangeCommentScript(objectType.MATERIALIZED_VIEW, view, sourceMaterializedViews[view].comment));
				} else {
					if (droppedViews.includes(view))
						//It will recreate a dropped materialized view because changes happens on involved columns
						sqlScript.push(sql.generateCreateMaterializedViewScript(view, sourceMaterializedViews[view]));

					sqlScript.push(
						...this.compareTableIndexes(sourceMaterializedViews[view].indexes, targetMaterializedViews[view].indexes, droppedIndexes)
					);

					sqlScript.push(
						...this.compareTablePrivileges(
							view,
							sourceMaterializedViews[view].privileges,
							targetMaterializedViews[view].privileges,
							config
						)
					);

					if (sourceMaterializedViews[view].owner != targetMaterializedViews[view].owner)
						sqlScript.push(sql.generateChangeTableOwnerScript(view, sourceMaterializedViews[view].owner));

					if (sourceMaterializedViews[view].comment != targetMaterializedViews[view].comment)
						sqlScript.push(sql.generateChangeCommentScript(objectType.MATERIALIZED_VIEW, view, sourceMaterializedViews[view].comment));
				}
			} else {
				//Materialized view not exists on target database, then generate the script to create materialized view
				actionLabel = "CREATE";

				sqlScript.push(sql.generateCreateMaterializedViewScript(view, sourceMaterializedViews[view]));
				sqlScript.push(sql.generateChangeCommentScript(objectType.MATERIALIZED_VIEW, view, sourceMaterializedViews[view].comment));
			}

			finalizedScript.push(...this.finalizeScript(`${actionLabel} MATERIALIZED VIEW ${view}`, sqlScript));
		}

		if (config.compareOptions.schemaCompare.dropMissingView)
			for (let view in targetMaterializedViews) {
				let sqlScript = [];

				if (!sourceMaterializedViews[view]) sqlScript.push(sql.generateDropMaterializedViewScript(view));

				finalizedScript.push(...this.finalizeScript(`DROP MATERIALIZED VIEW ${view}`, sqlScript));
			}

		return finalizedScript;
	}

	/**
	 *
	 * @param {Object} sourceFunctions
	 * @param {Object} targetFunctions
	 * @param {import("../models/config")} config
	 */
	static compareProcedures(sourceFunctions, targetFunctions, config) {
		let finalizedScript = [];

		for (let procedure in sourceFunctions) {
			for (const procedureArgs in sourceFunctions[procedure]) {
				let sqlScript = [];
				let actionLabel = "";
				const procedureType = sourceFunctions[procedure][procedureArgs].type === "f" ? objectType.FUNCTION : objectType.PROCEDURE;

				if (targetFunctions[procedure] && targetFunctions[procedure][procedureArgs]) {
					//Procedure exists on both database, then compare procedure definition
					actionLabel = "ALTER";

					//TODO: Is correct that if definition is different automatically GRANTS and OWNER will not be updated also?
					//TODO: Better to match only "visible" char in order to avoid special invisible like \t, spaces, etc;
					//      the problem is that a SQL STRING can contains special char as a fix from previous function version
					let sourceFunctionDefinition = sourceFunctions[procedure][procedureArgs].definition.replace(/\r/g, "");
					let targetFunctionDefinition = targetFunctions[procedure][procedureArgs].definition.replace(/\r/g, "");
					if (sourceFunctionDefinition != targetFunctionDefinition) {
						sqlScript.push(sql.generateChangeProcedureScript(procedure, sourceFunctions[procedure][procedureArgs]));
						sqlScript.push(
							sql.generateChangeCommentScript(
								procedureType,
								`${procedure}(${procedureArgs})`,
								sourceFunctions[procedure][procedureArgs].comment
							)
						);
					} else {
						sqlScript.push(
							...this.compareProcedurePrivileges(
								procedure,
								procedureArgs,
								sourceFunctions[procedure][procedureArgs].type,
								sourceFunctions[procedure][procedureArgs].privileges,
								targetFunctions[procedure][procedureArgs].privileges
							)
						);

						if (sourceFunctions[procedure][procedureArgs].owner != targetFunctions[procedure][procedureArgs].owner)
							sqlScript.push(
								sql.generateChangeProcedureOwnerScript(
									procedure,
									procedureArgs,
									sourceFunctions[procedure][procedureArgs].owner,
									sourceFunctions[procedure][procedureArgs].type
								)
							);

						if (sourceFunctions[procedure][procedureArgs].comment != sourceFunctions[procedure][procedureArgs].comment)
							sqlScript.push(
								sql.generateChangeCommentScript(
									procedureType,
									`${procedure}(${procedureArgs})`,
									sourceFunctions[procedure][procedureArgs].comment
								)
							);
					}
				} else {
					//Procedure not exists on target database, then generate the script to create procedure
					actionLabel = "CREATE";

					sqlScript.push(sql.generateCreateProcedureScript(procedure, sourceFunctions[procedure][procedureArgs]));
					sqlScript.push(
						sql.generateChangeCommentScript(
							procedureType,
							`${procedure}(${procedureArgs})`,
							sourceFunctions[procedure][procedureArgs].comment
						)
					);
				}

				finalizedScript.push(...this.finalizeScript(`${actionLabel} ${procedureType} ${procedure}(${procedureArgs})`, sqlScript));
			}
		}

		if (config.compareOptions.schemaCompare.dropMissingFunction)
			for (let procedure in targetFunctions) {
				for (const procedureArgs in targetFunctions[procedure]) {
					let sqlScript = [];

					if (!sourceFunctions[procedure] || !sourceFunctions[procedure][procedureArgs])
						sqlScript.push(sql.generateDropProcedureScript(procedure, procedureArgs));

					finalizedScript.push(...this.finalizeScript(`DROP FUNCTION ${procedure}(${procedureArgs})`, sqlScript));
				}
			}

		return finalizedScript;
	}

	/**
	 *
	 * @param {Object} sourceAggregates
	 * @param {Object} targetAggregates
	 * @param {import("../models/config")} config
	 */
	static compareAggregates(sourceAggregates, targetAggregates, config) {
		let finalizedScript = [];

		for (let aggregate in sourceAggregates) {
			for (const aggregateArgs in sourceAggregates[aggregate]) {
				let sqlScript = [];
				let actionLabel = "";

				if (targetAggregates[aggregate] && targetAggregates[aggregate][aggregateArgs]) {
					//Aggregate exists on both database, then compare procedure definition
					actionLabel = "ALTER";

					//TODO: Is correct that if definition is different automatically GRANTS and OWNER will not be updated also?
					if (sourceAggregates[aggregate][aggregateArgs].definition != targetAggregates[aggregate][aggregateArgs].definition) {
						sqlScript.push(sql.generateChangeAggregateScript(aggregate, sourceAggregates[aggregate][aggregateArgs]));
						sqlScript.push(
							sql.generateChangeCommentScript(
								objectType.AGGREGATE,
								`${aggregate}(${aggregateArgs})`,
								sourceAggregates[aggregate][aggregateArgs].comment
							)
						);
					} else {
						sqlScript.push(
							...this.compareProcedurePrivileges(
								aggregate,
								aggregateArgs,
								sourceAggregates[aggregate][aggregateArgs].type,
								sourceAggregates[aggregate][aggregateArgs].privileges,
								targetAggregates[aggregate][aggregateArgs].privileges
							)
						);

						if (sourceAggregates[aggregate][aggregateArgs].owner != targetAggregates[aggregate][aggregateArgs].owner)
							sqlScript.push(
								sql.generateChangeAggregateOwnerScript(aggregate, aggregateArgs, sourceAggregates[aggregate][aggregateArgs].owner)
							);

						if (sourceAggregates[aggregate][aggregateArgs].comment != targetAggregates[aggregate][aggregateArgs].comment)
							sqlScript.push(
								sql.generateChangeCommentScript(
									objectType.AGGREGATE,
									`${aggregate}(${aggregateArgs})`,
									sourceAggregates[aggregate][aggregateArgs].comment
								)
							);
					}
				} else {
					//Aggregate not exists on target database, then generate the script to create aggregate
					actionLabel = "CREATE";

					sqlScript.push(sql.generateCreateAggregateScript(aggregate, sourceAggregates[aggregate][aggregateArgs]));
					sqlScript.push(
						sql.generateChangeCommentScript(
							objectType.FUNCTION,
							`${aggregate}(${aggregateArgs})`,
							sourceAggregates[aggregate][aggregateArgs].comment
						)
					);
				}

				finalizedScript.push(...this.finalizeScript(`${actionLabel} AGGREGATE ${aggregate}(${aggregateArgs})`, sqlScript));
			}
		}

		if (config.compareOptions.schemaCompare.dropMissingAggregate)
			for (let aggregate in targetAggregates) {
				for (const aggregateArgs in targetAggregates[aggregate]) {
					let sqlScript = [];

					if (!sourceAggregates[aggregate] || !sourceAggregates[aggregate][aggregateArgs])
						sqlScript.push(sql.generateDropAggregateScript(aggregate, aggregateArgs));

					finalizedScript.push(...this.finalizeScript(`DROP AGGREGATE ${aggregate}(${aggregateArgs})`, sqlScript));
				}
			}

		return finalizedScript;
	}

	/**
	 *
	 * @param {String} procedure
	 * @param {String} argTypes
	 * @param {"f"|"p"} type
	 * @param {Object} sourceProcedurePrivileges
	 * @param {Object} targetProcedurePrivileges
	 */
	static compareProcedurePrivileges(procedure, argTypes, type, sourceProcedurePrivileges, targetProcedurePrivileges) {
		let sqlScript = [];

		for (let role in sourceProcedurePrivileges) {
			//Get new or changed role privileges
			if (targetProcedurePrivileges[role]) {
				//Procedure privileges for role exists on both database, then compare privileges
				let changes = {};
				if (sourceProcedurePrivileges[role].execute != targetProcedurePrivileges[role].execute)
					changes.execute = sourceProcedurePrivileges[role].execute;

				if (Object.keys(changes).length > 0)
					sqlScript.push(sql.generateChangesProcedureRoleGrantsScript(procedure, argTypes, role, changes, type));
			} else {
				//Procedure grants for role not exists on target database, then generate script to add role privileges
				sqlScript.push(sql.generateProcedureRoleGrantsScript(procedure, argTypes, role, sourceProcedurePrivileges[role], type));
			}
		}

		return sqlScript;
	}

	/**
	 *
	 * @param {Object} sourceSequences
	 * @param {Object} targetSequences
	 */
	static compareSequences(sourceSequences, targetSequences) {
		let finalizedScript = [];

		for (let sequence in sourceSequences) {
			let sqlScript = [];
			let actionLabel = "";
			let targetSequence =
				this.findRenamedSequenceOwnedByTargetTableColumn(sequence, sourceSequences[sequence].ownedBy, targetSequences) || sequence;

			if (targetSequences[targetSequence]) {
				//Sequence exists on both database, then compare sequence definition
				actionLabel = "ALTER";

				if (sequence != targetSequence)
					sqlScript.push(sql.generateRenameSequenceScript(targetSequence, `"${sourceSequences[sequence].name}"`));

				sqlScript.push(...this.compareSequenceDefinition(sequence, sourceSequences[sequence], targetSequences[targetSequence]));

				sqlScript.push(
					...this.compareSequencePrivileges(sequence, sourceSequences[sequence].privileges, targetSequences[targetSequence].privileges)
				);

				if (sourceSequences[sequence].comment != targetSequences[targetSequence].comment)
					sqlScript.push(sql.generateChangeCommentScript(objectType.SEQUENCE, sequence, sourceSequences[sequence].comment));
			} else {
				//Sequence not exists on target database, then generate the script to create sequence
				actionLabel = "CREATE";

				sqlScript.push(sql.generateCreateSequenceScript(sequence, sourceSequences[sequence]));
				sqlScript.push(sql.generateChangeCommentScript(objectType.SEQUENCE, sequence, sourceSequences[sequence].comment));
			}

			//TODO: @mso -> add a way to drop missing sequence if exists only on target db
			finalizedScript.push(...this.finalizeScript(`${actionLabel} SEQUENCE ${sequence}`, sqlScript));
		}

		return finalizedScript;
	}

	/**
	 *
	 * @param {String} sequenceName
	 * @param {String} tableColumn
	 * @param {Object} targetSequences
	 */
	static findRenamedSequenceOwnedByTargetTableColumn(sequenceName, tableColumn, targetSequences) {
		let result = null;

		for (let sequence in targetSequences.sequences) {
			if (targetSequences[sequence].ownedBy == tableColumn && sequence != sequenceName) {
				result = sequence;
				break;
			}
		}

		return result;
	}

	/**
	 *
	 * @param {String} sequence
	 * @param {Object} sourceSequenceDefinition
	 * @param {Object} targetSequenceDefinition
	 */
	static compareSequenceDefinition(sequence, sourceSequenceDefinition, targetSequenceDefinition) {
		let sqlScript = [];

		for (let property in sourceSequenceDefinition) {
			//Get new or changed properties

			if (property == "privileges" || property == "ownedBy" || property == "name" || property == "comment")
				//skip these properties from compare
				continue;

			if (sourceSequenceDefinition[property] != targetSequenceDefinition[property])
				sqlScript.push(sql.generateChangeSequencePropertyScript(sequence, property, sourceSequenceDefinition[property]));
		}

		return sqlScript;
	}

	/**
	 *
	 * @param {String} sequence
	 * @param {Object} sourceSequencePrivileges
	 * @param {Object} targetSequencePrivileges
	 */
	static compareSequencePrivileges(sequence, sourceSequencePrivileges, targetSequencePrivileges) {
		let sqlScript = [];

		for (let role in sourceSequencePrivileges) {
			//Get new or changed role privileges
			if (targetSequencePrivileges[role]) {
				//Sequence privileges for role exists on both database, then compare privileges
				let changes = {};
				if (sourceSequencePrivileges[role].select != targetSequencePrivileges[role].select)
					changes.select = sourceSequencePrivileges[role].select;

				if (sourceSequencePrivileges[role].usage != targetSequencePrivileges[role].usage)
					changes.usage = sourceSequencePrivileges[role].usage;

				if (sourceSequencePrivileges[role].update != targetSequencePrivileges[role].update)
					changes.update = sourceSequencePrivileges[role].update;

				if (Object.keys(changes).length > 0) sqlScript.push(sql.generateChangesSequenceRoleGrantsScript(sequence, role, changes));
			} else {
				//Sequence grants for role not exists on target database, then generate script to add role privileges
				sqlScript.push(sql.generateSequenceRoleGrantsScript(sequence, role, sourceSequencePrivileges[role]));
			}
		}

		return sqlScript;
	}

	/**
	 *
	 * @param {import("../models/config")} config
	 * @param {import("pg").Client} sourceClient
	 * @param {import("pg").Client} targetClient
	 * @param {Object} addedColumns
	 * @param {String[]} addedTables
	 * @param {import("../models/databaseObjects")} dbSourceObjects
	 * @param {import("../models/databaseObjects")} dbTargetObjects
	 * @param {import("events")} eventEmitter
	 */
	static async compareTablesRecords(config, sourceClient, targetClient, addedColumns, addedTables, dbSourceObjects, dbTargetObjects, eventEmitter) {
		let finalizedScript = [];
		let iteratorCounter = 0;
		let progressStepSize = Math.floor(20 / config.compareOptions.dataCompare.tables.length);

		for (let tableDefinition of config.compareOptions.dataCompare.tables) {
			let differentRecords = 0;
			let sqlScript = [];
			let fullTableName = `"${tableDefinition.tableSchema || "public"}"."${tableDefinition.tableName}"`;

			if (!(await this.checkIfTableExists(sourceClient, tableDefinition))) {
				sqlScript.push(`\n--ERROR: Table ${fullTableName} not found on SOURCE database for comparison!\n`);
			} else {
				let tableData = new TableData();
				tableData.sourceData.records = await this.collectTableRecords(sourceClient, tableDefinition, dbSourceObjects);
				tableData.sourceData.sequences = await this.collectTableSequences(sourceClient, tableDefinition);

				let isNewTable = false;
				if (addedTables.includes(fullTableName)) isNewTable = true;

				if (!isNewTable && !(await this.checkIfTableExists(targetClient, tableDefinition))) {
					sqlScript.push(
						`\n--ERROR: Table "${tableDefinition.tableSchema || "public"}"."${
							tableDefinition.tableName
						}" not found on TARGET database for comparison!\n`
					);
				} else {
					tableData.targetData.records = await this.collectTableRecords(targetClient, tableDefinition, dbTargetObjects, isNewTable);
					//  tableData.targetData.sequences = await this.collectTableSequences(targetClient, tableDefinition);

					let compareResult = this.compareTableRecords(tableDefinition, tableData, addedColumns);
					sqlScript.push(...compareResult.sqlScript);
					differentRecords = sqlScript.length;

					if (compareResult.isSequenceRebaseNeeded) sqlScript.push(...this.rebaseSequences(tableDefinition, tableData));
				}
			}
			finalizedScript.push(
				...this.finalizeScript(
					`SYNCHRONIZE TABLE "${tableDefinition.tableSchema || "public"}"."${tableDefinition.tableName}" RECORDS`,
					sqlScript
				)
			);

			iteratorCounter += 1;

			eventEmitter.emit(
				"compare",
				`Records for table ${fullTableName} have been compared with ${differentRecords} differences`,
				70 + progressStepSize * iteratorCounter
			);
		}

		return finalizedScript;
	}

	/**
	 *
	 * @param {import("pg").Client} client
	 * @param {import("../models/tableDefinition")} tableDefinition
	 * @returns {Promise<Boolean>}
	 */
	static async checkIfTableExists(client, tableDefinition) {
		let response = await client.query(
			`SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = '${tableDefinition.tableName}' AND schemaname = '${
				tableDefinition.tableSchema || "public"
			}')`
		);

		return response.rows[0].exists;
	}

	/**
	 *
	 * @param {import("pg").Client} client
	 * @param {import("../models/tableDefinition")} tableDefinition
	 * @param {import("../models/databaseObjects")} dbObjects
	 * @param {Boolean} isNewTable
	 */
	static async collectTableRecords(client, tableDefinition, dbObjects, isNewTable) {
		let result = {
			fields: [],
			rows: [],
		};

		if (!isNewTable) {
			let fullTableName = `"${tableDefinition.tableSchema || "public"}"."${tableDefinition.tableName}"`;

			let misssingKeyField = "";
			let missingKeyColumns = tableDefinition.tableKeyFields.some((k) => {
				if (!Object.keys(dbObjects.tables[fullTableName].columns).includes(`"${k}"`)) {
					misssingKeyField = k;
					return true;
				}
			});

			if (missingKeyColumns) throw new Error(`The table [${fullTableName}] doesn't contains the field [${misssingKeyField}]`);

			let response = await client.query(
				`SELECT MD5(ROW(${tableDefinition.tableKeyFields.map((c) => `"${c}"`).join(",")})::text) AS "rowHash", * FROM ${fullTableName}`
			);

			for (const field of response.fields) {
				if (field.name === "rowHash") continue;

				let f = field;
				f.datatype = dbObjects.tables[fullTableName].columns[`"${field.name}"`].datatype;
				f.dataTypeCategory = dbObjects.tables[fullTableName].columns[`"${field.name}"`].dataTypeCategory;
				f.isGeneratedColumn = dbObjects.tables[fullTableName].columns[`"${field.name}"`].generatedColumn ? true : false;
				result.fields.push(f);
			}

			result.rows = response.rows;
		}

		return result;
	}

	/**
	 *
	 * @param {import("pg").Client} client
	 * @param {import("../models/tableDefinition")} tableDefinition
	 */
	static async collectTableSequences(client, tableDefinition) {
		let identityFeature = `
        CASE 
            WHEN COALESCE(a.attidentity,'') = '' THEN 'SERIAL'
            WHEN a.attidentity = 'a' THEN 'ALWAYS'
            WHEN a.attidentity = 'd' THEN 'BY DEFAULT'
        END AS identitytype`;

		let response = await client.query(`
            SELECT * FROM (
                SELECT 
                    pg_get_serial_sequence(a.attrelid::regclass::name, a.attname) AS seqname,
                    a.attname,
                    ${client.version.major >= 10 ? identityFeature : "'SERIAL' AS identitytype"}
                FROM pg_attribute a
                WHERE a.attrelid = '"${tableDefinition.tableSchema || "public"}"."${tableDefinition.tableName}"'::regclass
                AND a.attnum > 0
                AND a.attisdropped = false
            ) T WHERE T.seqname IS NOT NULL`);

		return response.rows;
	}

	/**
	 *
	 * @param {import("../models/tableDefinition")} tableDefinition
	 * @param {import("../models/tableData")} tableData
	 * @param {Object} addedColumns
	 */
	static compareTableRecords(tableDefinition, tableData, addedColumns) {
		let ignoredRowHash = [];
		let result = {
			/** @type {String[]} */
			sqlScript: [],
			isSequenceRebaseNeeded: false,
		};
		let fullTableName = `"${tableDefinition.tableSchema || "public"}"."${tableDefinition.tableName}"`;

		//Check if at least one sequence is for an ALWAYS IDENTITY in case the OVERRIDING SYSTEM VALUE must be issued
		let isIdentityValuesAllowed = !tableData.sourceData.sequences.some((sequence) => sequence.identitytype === "ALWAYS");

		tableData.sourceData.records.rows.forEach((record, index) => {
			//Check if row hash has been ignored because duplicated or already processed from source
			if (ignoredRowHash.some((hash) => hash === record.rowHash)) return;

			let keyFieldsMap = this.getKeyFieldsMap(tableDefinition.tableKeyFields, record);

			//Check if record is duplicated in source
			if (tableData.sourceData.records.rows.some((r, idx) => r.rowHash === record.rowHash && idx > index)) {
				ignoredRowHash.push(record.rowHash);
				result.sqlScript.push(
					`\n--ERROR: Too many record found in SOURCE database for table ${fullTableName} and key fields ${JSON.stringify(
						keyFieldsMap
					)} !\n`
				);
				return;
			}

			//Check if record is duplicated in target
			let targetRecord = [];
			targetRecord = tableData.targetData.records.rows.filter(function (r) {
				return r.rowHash === record.rowHash;
			});

			if (targetRecord.length > 1) {
				ignoredRowHash.push(record.rowHash);
				result.sqlScript.push(
					`\n--ERROR: Too many record found in TARGET database for table ${fullTableName} and key fields ${JSON.stringify(
						keyFieldsMap
					)} !\n`
				);
				return;
			}

			ignoredRowHash.push(record.rowHash);

			//Generate sql script to add\update record in target database table
			if (targetRecord.length <= 0) {
				//A record with same KEY FIELDS not exists, then create a new record
				delete record.rowHash; //Remove property from "record" object in order to not add it on sql script
				result.sqlScript.push(
					sql.generateInsertTableRecordScript(fullTableName, record, tableData.sourceData.records.fields, isIdentityValuesAllowed)
				);
				result.isSequenceRebaseNeeded = true;
			} else {
				//A record with same KEY FIELDS VALUES has been found, then update not matching fieds only
				let fieldCompareResult = this.compareTableRecordFields(
					fullTableName,
					keyFieldsMap,
					tableData.sourceData.records.fields,
					record,
					targetRecord[0],
					addedColumns
				);
				if (fieldCompareResult.isSequenceRebaseNeeded) result.isSequenceRebaseNeeded = true;
				result.sqlScript.push(...fieldCompareResult.sqlScript);
			}
		});

		tableData.targetData.records.rows.forEach((record, index) => {
			//Check if row hash has been ignored because duplicated or already processed from source
			if (ignoredRowHash.some((hash) => hash === record.rowHash)) return;

			let keyFieldsMap = this.getKeyFieldsMap(tableDefinition.tableKeyFields, record);

			if (tableData.targetData.records.rows.some((r, idx) => r.rowHash === record.rowHash && idx > index)) {
				ignoredRowHash.push(record.rowHash);
				result.sqlScript.push(
					`\n--ERROR: Too many record found in TARGET database for table ${fullTableName} and key fields ${JSON.stringify(
						keyFieldsMap
					)} !\n`
				);
				return;
			}

			//Generate sql script to delete record because not exists on source database table
			result.sqlScript.push(sql.generateDeleteTableRecordScript(fullTableName, tableData.sourceData.records.fields, keyFieldsMap));
			result.isSequenceRebaseNeeded = true;
		});

		return result;
	}

	/**
	 *
	 * @param {String[]} keyFields
	 * @param {Object} record
	 */
	static getKeyFieldsMap(keyFields, record) {
		let keyFieldsMap = {};
		keyFields.forEach((item) => {
			keyFieldsMap[item] = record[item];
		});
		return keyFieldsMap;
	}

	/**
	 *
	 * @param {String} table
	 * @param {Object} keyFieldsMap
	 * @param {Array} fields
	 * @param {Object} sourceRecord
	 * @param {Object} targetRecord
	 * @param {Object} addedColumns
	 */
	static compareTableRecordFields(table, keyFieldsMap, fields, sourceRecord, targetRecord, addedColumns) {
		let changes = {};
		let result = {
			/** @type {String[]} */
			sqlScript: [],
			isSequenceRebaseNeeded: false,
		};

		for (const field in sourceRecord) {
			if (field === "rowHash") continue;
			if (fields.some((f) => f.name == field && f.isGeneratedColumn == true)) {
				continue;
			}

			if (targetRecord[field] === undefined && this.checkIsNewColumn(addedColumns, table, field)) {
				changes[field] = sourceRecord[field];
			} else if (this.compareFieldValues(sourceRecord[field], targetRecord[field])) {
				changes[field] = sourceRecord[field];
			}
		}

		if (Object.keys(changes).length > 0) {
			result.isSequenceRebaseNeeded = true;
			result.sqlScript.push(sql.generateUpdateTableRecordScript(table, fields, keyFieldsMap, changes));
		}

		return result;
	}

	/**
	 *
	 * @param {Object} addedColumns
	 * @param {String} table
	 * @param {String} field
	 */
	static checkIsNewColumn(addedColumns, table, field) {
		if (
			addedColumns[table] &&
			addedColumns[table].some((column) => {
				return column == field;
			})
		)
			return true;
		else return false;
	}

	/**
	 *
	 * @param {Object} sourceValue
	 * @param {Object} targetValue
	 */
	static compareFieldValues(sourceValue, targetValue) {
		var sourceValueType = typeof sourceValue;
		var targetValueType = typeof targetValue;

		if (sourceValueType != targetValueType) return false;
		else if (sourceValue instanceof Date) return sourceValue.getTime() !== targetValue.getTime();
		else if (sourceValue instanceof Object) return !deepEqual(sourceValue, targetValue);
		else return sourceValue !== targetValue;
	}

	/**
	 *
	 * @param {import("../models/tableDefinition")} tableDefinition
	 * @param {import("../models/tableData")} tableData
	 */
	static rebaseSequences(tableDefinition, tableData) {
		let sqlScript = [];
		let fullTableName = `"${tableDefinition.tableSchema || "public"}"."${tableDefinition.tableName}"`;

		tableData.sourceData.sequences.forEach((sequence) => {
			sqlScript.push(sql.generateSetSequenceValueScript(fullTableName, sequence));
		});

		return sqlScript;
	}

	/**
	 *
	 * @param {String[]} scriptLines
	 * @param {import("../models/config")} config
	 * @param {import("events")} eventEmitter
	 * @returns {String}
	 */
	static async saveSqlScript(scriptLines, config, scriptName, eventEmitter) {
		if (scriptLines.length <= 0) return null;

		const now = new Date();
		const fileName = `${now.toISOString().replace(/[-:.TZ]/g, "")}_${scriptName}.sql`;

		if (typeof config.compareOptions.outputDirectory !== "string" && !(config.compareOptions.outputDirectory instanceof String))
			config.compareOptions.outputDirectory = "";

		const scriptPath = path.resolve(config.compareOptions.outputDirectory || "", fileName);
		if (config.compareOptions.getAuthorFromGit) {
			config.compareOptions.author = await core.getGitAuthor();
		}
		const datetime = now.toISOString();
		const titleLength = config.compareOptions.author.length > now.toISOString().length ? config.compareOptions.author.length : datetime.length;

		return new Promise((resolve, reject) => {
			try {
				var file = fs.createWriteStream(scriptPath);

				file.on("error", reject);

				file.on("finish", () => {
					eventEmitter.emit("compare", "Patch file have been created", 99);
					resolve(scriptPath);
				});

				file.write(`/******************${"*".repeat(titleLength + 2)}***/\n`);
				file.write(`/*** SCRIPT AUTHOR: ${config.compareOptions.author.padEnd(titleLength)} ***/\n`);
				file.write(`/***    CREATED ON: ${datetime.padEnd(titleLength)} ***/\n`);
				file.write(`/******************${"*".repeat(titleLength + 2)}***/\n`);

				scriptLines.forEach(function (line) {
					file.write(line);
				});

				file.end();
			} catch (err) {
				reject(err);
			}
		});
	}
}

module.exports = CompareApi;
