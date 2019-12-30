const core = require("../core");
const catalogApi = require("./CatalogApi");
const DatabaseObjects = require("../models/databaseObjects");
const sql = require("../sqlScriptGenerator");

class CompareApi {
    /**
     *
     * @param {import("../models/config")} config
     */
    static async compare(config) {
        let pgSourceClient = await core.makePgClient(config.sourceClient);
        let pgTargetClient = await core.makePgClient(config.targetClient);

        let dbSourceObjects = await this.collectSchemaObjects(pgSourceClient, config.compareOptions.schemaCompare.namespaces);
        let dbTargetObjects = await this.collectSchemaObjects(pgTargetClient, config.compareOptions.schemaCompare.namespaces);

        let droppedConstraints = [];
        let droppedIndexes = [];
        let droppedViews = [];
        let addedColumns = [];

        let scripts = this.compareDatabaseObjects(
            dbSourceObjects,
            dbTargetObjects,
            droppedConstraints,
            droppedIndexes,
            droppedViews,
            addedColumns,
            config,
        );

        if (config.compareOptions.dataCompare.enable) {
            let dataTypes = (await sourceClient.query(`SELECT oid, typcategory, typname FROM pg_type`)).rows;

            let sourceTablesRecords = await data.collectTablesRecords(sourceClient, global.config.options.dataCompare.tables);

            log();
            log();
            log(chalk.yellow("Collect TARGET tables records"));
            let targetTablesRecords = await data.collectTablesRecords(targetClient, global.config.options.dataCompare.tables);

            log();
            log();
            log(chalk.yellow("Compare SOURCE with TARGET database table records"));
            scripts = scripts.concat(
                compareRecords.compareTablesRecords(global.config.options.dataCompare.tables, sourceTablesRecords, targetTablesRecords),
            );
        } else {
            log();
            log();
            log(chalk.yellow("Data compare not enabled!"));
        }

        let scriptFilePath = await __saveSqlScript(scripts);

        log();
        log();
        log(chalk.whiteBright("SQL patch file has been created succesfully at: ") + chalk.green(scriptFilePath));

        process.exit();
    }

    /**
     *
     * @param {import("pg").Client} client
     * @param {String[]} schemas
     * @returns {Promise<import("../models/databaseObjects")>}
     */
    static async collectSchemaObjects(client, schemas) {
        return new Promise(async (resolve, reject) => {
            try {
                var dbObjects = new DatabaseObjects();

                dbObjects.schemas = await catalogApi.retrieveSchemas(client, schemas);
                dbObjects.tables = await catalogApi.retrieveTables(client, schemas);
                dbObjects.views = await catalogApi.retrieveViews(client, schemas);
                dbObjects.materializedViews = await catalogApi.retrieveMaterializedViews(client, schemas);
                dbObjects.functions = await catalogApi.retrieveFunctions(client, schemas);
                dbObjects.sequences = await catalogApi.retrieveSequences(client, schemas);

                //TODO: Do we need to retrieve data types?
                //TODO: Do we need to retrieve roles?
                //TODO: Do we need to retieve special table like TEMPORARY and UNLOGGED? for sure not temporary, but UNLOGGED probably yes.
                //TODO: Do we need to retrieve collation for both table and columns?

                resolve(dbObjects);
            } catch (e) {
                reject(e);
            }
        });
    }

    /**
     *
     * @param {import("../models/databaseObjects")} dbSourceObjects
     * @param {import("../models/databaseObjects")} dbTargetObjects
     * @param {String[]} droppedConstraints
     * @param {String[]} droppedIndexes
     * @param {String[]} droppedViews
     * @param {Array} addedColumns
     * @param {import("../models/config")} config
     */
    static compareDatabaseObjects(dbSourceObjects, dbTargetObjects, droppedConstraints, droppedIndexes, droppedViews, addedColumns, config) {
        let sqlPatch = [];

        sqlPatch.push(...this.compareSchemas(dbSourceObjects.schemas, dbTargetObjects.schemas));
        sqlPatch.push(
            ...this.compareTables(dbSourceObjects.tables, dbTargetObjects, droppedConstraints, droppedIndexes, droppedViews, addedColumns, config),
        );
        sqlPatch.push(...this.compareViews(dbSourceObjects.views, dbTargetObjects.views, droppedViews, config));
        sqlPatch.push(...this.compareMaterializedViews(dbSourceObjects.materializedViews, dbTargetObjects.materializedViews, droppedViews, config));
        sqlPatch.push(...this.compareProcedures(dbSourceObjects.functions, dbTargetObjects.functions, config));
        sqlPatch.push(...this.compareSequences(dbSourceObjects.sequences, dbTargetObjects.sequences));

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
            }

            finalizedScript.push(...this.finalizeScript(`CREATE SCHEMA ${schema}`, sqlScript));
        }

        return finalizedScript;
    }

    /**
     *
     * @param {Array} sourceTables
     * @param {import("../models/databaseObjects")} dbTargetObjects
     * @param {String[]} droppedConstraints
     * @param {String[]} droppedIndexes
     * @param {String[]} droppedViews
     * @param {Array} addedColumns
     * @param {import("../models/config")} config
     */
    static compareTables(sourceTables, dbTargetObjects, droppedConstraints, droppedIndexes, droppedViews, addedColumns, config) {
        let finalizedScript = [];

        for (let sourceTable in sourceTables) {
            let sqlScript = [];
            let actionLabel = "";

            if (dbTargetObjects.tables[sourceTable]) {
                //Table exists on both database, then compare table schema
                actionLabel = "ALTER";

                sqlScript.push(this.compareTableOptions(sourceTable, sourceTables[sourceTable].options, targetTables[sourceTable].options));
                sqlScript.push(
                    ...this.compareTableColumns(
                        sourceTable,
                        sourceTables[sourceTable].columns,
                        dbTargetObjects,
                        droppedConstraints,
                        droppedIndexes,
                        droppedViews,
                        addedColumns,
                    ),
                );
                sqlScript.push(
                    ...this.compareTableConstraints(
                        sourceTable,
                        sourceTables[sourceTable].constraints,
                        dbTargetObjects.tables[sourceTable].constraints,
                        droppedConstraints,
                    ),
                );
                sqlScript.push(
                    ...this.compareTableIndexes(sourceTables[sourceTable].indexes, dbTargetObjects.tables[sourceTable].indexes, droppedIndexes),
                );
                sqlScript.push(
                    ...this.compareTablePrivileges(sourceTable, sourceTables[sourceTable].privileges, dbTargetObjects.tables[sourceTable].privileges),
                );

                if (sourceTables[sourceTable].owner != dbTargetObjects.tables[sourceTable].owner)
                    sqlScript.push(sql.generateChangeTableOwnerScript(sourceTable, sourceTables[sourceTable].owner));
            } else {
                //Table not exists on target database, then generate the script to create table
                actionLabel = "CREATE";

                sqlScript.push(sql.generateCreateTableScript(sourceTable, sourceTables[sourceTable]));
            }

            finalizedScript.push(...this.finalizeScript(`${actionLabel} TABLE ${sourceTable}`, sqlScript));
        }

        if (config.compareOptions.schemaCompare.dropMissingTable)
            for (let table in dbTargetObjects.tables) {
                let sqlScript = [];

                if (!sourceTables.hasOwnProperty(table)) sqlScript.push(sql.generateDropTableScript(table));

                finalizedScript.push(...this.finalizeScript(`DROP TABLE ${table}`, sqlScript));
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
        if (sourceTableOptions.withOids != targetTableOptions.withOids) return sql.generateChangeTableOptionsScript(tableName, sourceTableOptions);
        else return "";
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

        for (let sourceTableColumn in sourceTableColumns) {
            let targetTable = dbTargetObjects.tables[tableName];

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
                        droppedViews,
                    ),
                );
            } else {
                //Table column not exists on target database, then generate script to add column
                sqlScript.push(sql.generateAddTableColumnScript(tableName, sourceTableColumn, sourceTableColumns[column]));
                if (!addedColumns[table]) addedColumns[table] = [];

                addedColumns[table].push(sourceTableColumn);
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

        if (Object.keys(changes).length > 0) {
            let rawColumnName = columnName.substring(1).slice(0, -1);

            //Check if the column is has constraint
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
                dbTargetObjects.views[view].dependencies.forEach(dependency => {
                    let fullDependencyName = `"${dependency.schemaName}"."${dependency.tableName}"`;
                    if (fullDependencyName == tableName && dependency.columnName == columnName) {
                        sqlScript.push(sql.generateDropViewScript(index));
                        droppedViews.push(view);
                    }
                });
            }

            //Check if the column is used into materialized view
            for (let view in dbTargetObjects.materializedViews) {
                dbTargetObjects.materializedViews[view].dependencies.forEach(dependency => {
                    let fullDependencyName = `"${dependency.schemaName}"."${dependency.tableName}"`;
                    if (fullDependencyName == tableName && dependency.columnName == columnName) {
                        sqlScript.push(sql.generateDropMaterializedViewScript(index));
                        droppedViews.push(view);
                    }
                });
            }

            sqlScript.push(sql.generateChangeTableColumnScript(table, column, changes));
        }

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
                } else {
                    if (droppedConstraints.includes(constraint))
                        //It will recreate a dropped constraints because changes happens on involved columns
                        sqlScript.push(sql.generateAddTableConstraintScript(tableName, constraint, sourceTableConstraints[constraint]));
                }
            } else {
                //Table constraint not exists on target database, then generate script to add constraint
                sqlScript.push(sql.generateAddTableConstraintScript(tableName, constraint, sourceTableConstraints[constraint]));
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
                } else {
                    if (droppedIndexes.includes(index))
                        //It will recreate a dropped index because changes happens on involved columns
                        sqlScript.push(`\n${sourceTableIndexes[index].definition};\n`);
                }
            } else {
                //Table index not exists on target database, then generate script to add index
                sqlScript.push(`\n${sourceTableIndexes[index].definition};\n`);
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
     */
    static compareTablePrivileges(tableName, sourceTablePrivileges, targetTablePrivileges) {
        let sqlScript = [];

        for (let role in sourceTablePrivileges) {
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

                if (sourceViews[view].definition != targetViews[view].definition) {
                    if (!droppedViews.includes(view)) sqlScript.push(sql.generateDropViewScript(view));
                    sqlScript.push(sql.generateCreateViewScript(view, sourceViews[view]));
                } else {
                    if (droppedViews.includes(view))
                        //It will recreate a dropped view because changes happens on involved columns
                        sqlScript.push(sql.generateCreateViewScript(view, sourceViews[view]));

                    sqlScript.push(...this.compareTablePrivileges(view, sourceViews[view].privileges, targetViews[view].privileges));
                    if (sourceViews[view].owner != targetViews[view].owner)
                        sqlScript.push(sql.generateChangeTableOwnerScript(view, sourceViews[view].owner));
                }
            } else {
                //View not exists on target database, then generate the script to create view
                actionLabel = "CREATE";

                sqlScript.push(sql.generateCreateViewScript(view, sourceViews[view]));
            }

            finalizedScript.push(...this.finalizeScript(`${actionLabel} VIEW ${view}`, sqlScript));
        }

        if (config.compareOptions.schemaCompare.dropMissingView)
            for (let view in targetViews) {
                //Get missing views
                let sqlScript = [];

                if (!sourceViews.hasOwnProperty(view)) sqlScript.push(sql.generateDropViewScript(view));

                finalizedScript.push(...this.finalizeScript(`DROP VIEW ${view}`, sqlScript));
            }

        return finalizedScript;
    }

    /**
     *
     * @param {Object} sourceMaterializedViews
     * @param {Object} targetMaterializedViews
     * @param {String[]} droppedViews
     * @param {import("../models/config")} config
     */
    static compareMaterializedViews(sourceMaterializedViews, targetMaterializedViews, droppedViews, config) {
        let finalizedScript = [];

        for (let view in sourceMaterializedViews) {
            //Get new or changed materialized views
            let sqlScript = [];
            let actionLabel = "";

            if (targetMaterializedViews[view]) {
                //Materialized view exists on both database, then compare materialized view schema
                actionLabel = "ALTER";

                if (sourceMaterializedViews[view].definition != targetMaterializedViews[view].definition) {
                    if (!droppedViews.includes(view)) sqlScript.push(sql.generateDropMaterializedViewScript(view));
                    sqlScript.push(sql.generateCreateMaterializedViewScript(view, sourceMaterializedViews[view]));
                } else {
                    if (droppedViews.includes(view))
                        //It will recreate a dropped materialized view because changes happens on involved columns
                        sqlScript.push(sql.generateCreateMaterializedViewScript(view, sourceMaterializedViews[view]));

                    sqlScript.push(
                        ...this.compareTableIndexes(sourceMaterializedViews[view].indexes, targetMaterializedViews[view].indexes, droppedIndexes),
                    );

                    sqlScript.push(
                        ...this.compareTablePrivileges(view, sourceMaterializedViews[view].privileges, targetMaterializedViews[view].privileges),
                    );

                    if (sourceMaterializedViews[view].owner != targetMaterializedViews[view].owner)
                        sqlScript.push(sql.generateChangeTableOwnerScript(view, sourceMaterializedViews[view].owner));
                }
            } else {
                //Materialized view not exists on target database, then generate the script to create materialized view
                actionLabel = "CREATE";

                sqlScript.push(sql.generateCreateMaterializedViewScript(view, sourceMaterializedViews[view]));
            }

            finalizedScript.push(...this.finalizeScript(`${actionLabel} MATERIALIZED VIEW ${view}`, sqlScript));
        }

        if (config.compareOptions.schemaCompare.dropMissingView)
            for (let view in targetMaterializedViews) {
                let sqlScript = [];

                if (!sourceMaterializedViews.hasOwnProperty(view)) sqlScript.push(sql.generateDropMaterializedViewScript(view));

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
            let sqlScript = [];
            let actionLabel = "";

            if (targetFunctions[procedure]) {
                //Procedure exists on both database, then compare procedure definition
                actionLabel = "ALTER";

                //TODO: Is correct that if definition is different automatically GRANTS and OWNER will not be updated also?
                if (sourceFunctions[procedure].definition != targetFunctions[procedure].definition) {
                    sqlScript.push(sql.generateChangeProcedureScript(procedure, sourceFunctions[procedure]));
                } else {
                    sqlScript.push(
                        ...this.compareProcedurePrivileges(
                            procedure,
                            sourceFunctions[procedure].argTypes,
                            sourceFunctions[procedure].privileges,
                            targetFunctions[procedure].privileges,
                        ),
                    );

                    if (sourceFunctions[procedure].owner != targetFunctions[procedure].owner)
                        sqlScript.push(
                            sql.generateChangeProcedureOwnerScript(procedure, sourceFunctions[procedure].argTypes, sourceFunctions[procedure].owner),
                        );
                }
            } else {
                //Procedure not exists on target database, then generate the script to create procedure
                actionLabel = "CREATE";

                sqlScript.push(sql.generateCreateProcedureScript(procedure, this.__sourceSchema.functions[procedure]));
            }

            finalizedScript.push(...this.finalizeScript(`${actionLabel} FUNCTION ${procedure}`, sqlScript));
        }

        if (config.compareOptions.schemaCompare.dropMissingFunction)
            for (let procedure in targetFunctions) {
                let sqlScript = [];

                if (!sourceFunctions.hasOwnProperty(procedure)) sqlScript.push(sql.generateDropProcedureScript(procedure));

                finalizedScript.push(...this.finalizeScript(`DROP FUNCTION ${procedure}`, sqlScript));
            }

        return finalizedScript;
    }

    /**
     *
     * @param {String} procedure
     * @param {String} argTypes
     * @param {Object} sourceProcedurePrivileges
     * @param {Object} targetProcedurePrivileges
     */
    static compareProcedurePrivileges(procedure, argTypes, sourceProcedurePrivileges, targetProcedurePrivileges) {
        let sqlScript = [];

        for (let role in sourceProcedurePrivileges) {
            //Get new or changed role privileges
            if (targetProcedurePrivileges[role]) {
                //Procedure privileges for role exists on both database, then compare privileges
                let changes = {};
                if (sourceProcedurePrivileges[role].execute != targetProcedurePrivileges[role].execute)
                    changes.execute = sourceProcedurePrivileges[role].execute;

                if (Object.keys(changes).length > 0) sqlScript.push(sql.generateChangesProcedureRoleGrantsScript(procedure, argTypes, role, changes));
            } else {
                //Procedure grants for role not exists on target database, then generate script to add role privileges
                sqlScript.push(sql.generateProcedureRoleGrantsScript(procedure, argTypes, role, sourceProcedurePrivileges[role]));
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
            let renamedOwnedSequence = this.findRenamedSequenceOwnedByTargetTableColumn(sequence, sourceSequences[sequence].ownedBy, targetSequences);

            if (renamedOwnedSequence) {
                actionLabel = "ALTER";

                sqlScript.push(sql.generateRenameSequenceScript(renamedOwnedSequence, `"${sourceSequences[sequence].name}"`));

                sqlScript.push(...this.compareSequenceDefinition(sequence, sourceSequences[sequence], targetSequences[renamedOwnedSequence]));

                sqlScript.push(
                    ...this.compareSequencePrivileges(
                        sequence,
                        sourceSequences[sequence].privileges,
                        targetSequences[renamedOwnedSequence].privileges,
                    ),
                );
            } else if (targetSequences[sequence]) {
                //Sequence exists on both database, then compare sequence definition
                actionLabel = "ALTER";

                sqlScript.push(...this.compareSequenceDefinition(sequence, sourceSequences[sequence], targetSequences[sequence]));

                sqlScript.push(
                    ...this.compareSequencePrivileges(sequence, sourceSequences[sequence].privileges, targetSequences[sequence].privileges),
                );
            } else {
                //Sequence not exists on target database, then generate the script to create sequence
                actionLabel = "CREATE";

                sqlScript.push(sql.generateCreateSequenceScript(sequence, this.__sourceSchema.sequences[sequence]));
            }
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

            if (property == "privileges" || property == "ownedBy" || property == "name")
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
}

module.exports = CompareApi;
