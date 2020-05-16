const core = require("../core");

const query = {
	/**
	 *
	 * @param {String[]} schemas
	 */
	getSchemas: function (schemas) {
		//TODO: Instead of using ::regrole casting, for better performance join with pg_roles
		return `SELECT nspname, nspowner::regrole::name as owner FROM pg_namespace WHERE nspname IN ('${schemas.join("','")}')`;
	},
	/**
	 *
	 * @param {String[]} schemas
	 */
	getTables: function (schemas) {
		return `SELECT schemaname, tablename, tableowner 
                FROM pg_tables t
                INNER JOIN pg_class c on t.tablename::regclass = c.oid 
                WHERE t.schemaname IN ('${schemas.join("','")}')
                AND c.oid NOT IN (
                    SELECT d.objid 
                    FROM pg_depend d
                    WHERE d.deptype = 'e'
                )`;
	},
	/**
	 *
	 * @param {String} tableName
	 */
	getTableOptions: function (tableName) {
		//TODO: Instead of using ::regnamespace casting, for better performance join with pg_namespace
		return `SELECT relhasoids FROM pg_class WHERE oid = '${tableName}'::regclass`;
	},
	/**
	 *
	 * @param {String} tableName
	 * @param {import("../models/serverVersion")} serverVersion
	 */
	getTableColumns: function (tableName, serverVersion) {
		//TODO: Instead of using ::regclass casting, for better performance join with pg_class
		return `SELECT a.attname, a.attnotnull, t.typname, t.oid as typeid, t.typcategory, pg_get_expr(ad.adbin ,ad.adrelid ) as adsrc, ${
			core.checkServerCompatibility(serverVersion, 10, 0) ? "a.attidentity" : "NULL as attidentity"
		},
                CASE 
                    WHEN t.typname = 'numeric' AND a.atttypmod > 0 THEN (a.atttypmod-4) >> 16
                    WHEN (t.typname = 'bpchar' or t.typname = 'varchar') AND a.atttypmod > 0 THEN a.atttypmod-4
                    ELSE null
                END AS precision,
                CASE
                    WHEN t.typname = 'numeric' AND a.atttypmod > 0 THEN (a.atttypmod-4) & 65535
                    ELSE null
                END AS scale 	
                FROM pg_attribute a
                INNER JOIN pg_type t ON t.oid = a.atttypid
                LEFT JOIN pg_attrdef ad on ad.adrelid = a.attrelid AND a.attnum = ad.adnum
                WHERE attrelid = '${tableName}'::regclass AND attnum > 0 AND attisdropped = false
                ORDER BY a.attnum ASC`;
	},
	/**
	 *
	 * @param {String} tableName
	 */
	getTableConstraints: function (tableName) {
		//TODO: Instead of using ::regclass casting, for better performance join with pg_class
		return `SELECT conname, contype, pg_get_constraintdef(c.oid) as definition
                FROM pg_constraint c
                WHERE c.conrelid = '${tableName}'::regclass`;
	},
	/**
	 *
	 * @param {String} schemaName
	 * @param {String} tableName
	 */
	getTableIndexes: function (schemaName, tableName) {
		//TODO: Instead of using ::regnamespace casting, for better performance join with pg_namespace
		return `SELECT idx.relname as indexname, pg_get_indexdef(idx.oid) AS indexdef
                FROM pg_index i
                INNER JOIN pg_class tbl ON tbl.oid = i.indrelid
                INNER JOIN pg_class idx ON idx.oid = i.indexrelid
                WHERE tbl.relnamespace = '"${schemaName}"'::regnamespace::oid and tbl.relname='${tableName}' and i.indisprimary = false`;
	},
	/**
	 *
	 * @param {String} schemaName
	 * @param {String} tableName
	 */
	getTablePrivileges: function (schemaName, tableName) {
		return `SELECT t.schemaname, t.tablename, u.usename, 
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${tableName}"', 'SELECT') as select,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${tableName}"', 'INSERT') as insert,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${tableName}"', 'UPDATE') as update,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${tableName}"', 'DELETE') as delete, 
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${tableName}"', 'TRUNCATE') as truncate,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${tableName}"', 'REFERENCES') as references,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${tableName}"', 'TRIGGER') as trigger
                FROM pg_tables t, pg_user u 
                WHERE t.schemaname = '${schemaName}' and t.tablename='${tableName}'`;
	},
	/**
	 *
	 * @param {String[]} schemas
	 */
	getViews: function (schemas) {
		return `SELECT schemaname, viewname, viewowner, definition 
                FROM pg_views v
                INNER JOIN pg_class c on v.viewname::regclass = c.oid 
                WHERE v.schemaname IN ('${schemas.join("','")}')
                AND c.oid NOT IN (
                    SELECT d.objid 
                    FROM pg_depend d
                    WHERE d.deptype = 'e'
                )`;
	},
	/**
	 *
	 * @param {String} schemaName
	 * @param {String} viewName
	 */
	getViewPrivileges: function (schemaName, viewName) {
		return `SELECT v.schemaname, v.viewname, u.usename, 
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'SELECT') as select,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'INSERT') as insert,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'UPDATE') as update,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'DELETE') as delete, 
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'TRUNCATE') as truncate,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'REFERENCES') as references,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'TRIGGER') as trigger
                FROM pg_views v, pg_user u 
                WHERE v.schemaname = '${schemaName}' and v.viewname='${viewName}'`;
	},
	/**
	 *
	 * @param {String[]} schemas
	 */
	getMaterializedViews: function (schemas) {
		return `SELECT schemaname, matviewname, matviewowner, definition FROM pg_matviews WHERE schemaname IN ('${schemas.join("','")}')`;
	},
	/**
	 *
	 * @param {String} schemaName
	 * @param {String} viewName
	 */
	getMaterializedViewPrivileges: function (schemaName, viewName) {
		return `SELECT v.schemaname, v.matviewname, u.usename, 
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'SELECT') as select,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'INSERT') as insert,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'UPDATE') as update,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'DELETE') as delete, 
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'TRUNCATE') as truncate,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'REFERENCES') as references,
                HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'TRIGGER') as trigger
                FROM pg_matviews v, pg_user u 
                WHERE v.schemaname = '${schemaName}' and v.matviewname='${viewName}'`;
	},
	/**
	 *
	 * @param {String} schemaName
	 * @param {String} viewName
	 */
	getViewDependencies: function (schemaName, viewName) {
		//TODO: Instead of using ::regclass casting, for better performance join with pg_class
		return `SELECT                 
                n.nspname AS schemaname,
                c.relname AS tablename,
                a.attname AS columnname
                FROM pg_rewrite AS r
                INNER JOIN pg_depend AS d ON r.oid=d.objid
                INNER JOIN pg_attribute a ON a.attnum = d.refobjsubid AND a.attrelid = d.refobjid AND a.attisdropped = false
                INNER JOIN pg_class c ON c.oid = d.refobjid
                INNER JOIN pg_namespace n ON n.oid = c.relnamespace                
                WHERE r.ev_class='"${schemaName}"."${viewName}"'::regclass::oid AND d.refobjid <> '"${schemaName}"."${viewName}"'::regclass::oid`;
	},
	/**
	 *
	 * @param {String[]} schemas
	 */
	getFunctions: function (schemas, serverVersion) {
		//TODO: Instead of using ::regrole casting, for better performance join with pg_roles
		return `SELECT p.proname, n.nspname, pg_get_functiondef(p.oid) as definition, p.proowner::regrole::name as owner, oidvectortypes(proargtypes) as argtypes
                FROM pg_proc p
                INNER JOIN pg_namespace n ON n.oid = p.pronamespace
				WHERE n.nspname IN ('${schemas.join("','")}') AND p.probin IS NULL ${
			core.checkServerCompatibility(serverVersion, 11, 0) ? "AND p.prokind = 'f'" : "AND p.proisagg = false AND p.proiswindow = false"
		} AND p."oid" NOT IN (
                    SELECT d.objid 
                    FROM pg_depend d
                    WHERE d.deptype = 'e'
                )`;
	},
	/**
	 *
	 * @param {String} schemaName
	 * @param {String} functionName
	 * @param {String} argTypes
	 */
	getFunctionPrivileges: function (schemaName, functionName, argTypes) {
		//TODO: Instead of using ::regnamespace casting, for better performance join with pg_namespace
		return `SELECT p.pronamespace::regnamespace::name, p.proname, u.usename, 
                HAS_FUNCTION_PRIVILEGE(u.usename,'"${schemaName}"."${functionName}"(${argTypes})','EXECUTE') as execute  
                FROM pg_proc p, pg_user u 
                WHERE p.proname='${functionName}' AND p.pronamespace::regnamespace = '"${schemaName}"'::regnamespace`;
	},
	/**
	 *
	 * @param {String[]} schemas
	 * @param {import("../models/serverVersion")} serverVersion
	 */
	getSequences: function (schemas, serverVersion) {
		return `SELECT seq_nspname, seq_name, owner, ownedby_table, ownedby_column, p.start_value, p.minimum_value, p.maximum_value, p.increment, p.cycle_option, ${
			core.checkServerCompatibility(serverVersion, 10, 0) ? "p.cache_size" : "1 as cache_size"
		}
                FROM (
                    SELECT   
                        c.oid, ns.nspname AS seq_nspname, c.relname AS seq_name, r.rolname as owner, d.refobjid::regclass AS ownedby_table, a.attname AS ownedby_column	    
                    FROM pg_class c
                    INNER JOIN pg_namespace ns ON ns.oid = c.relnamespace 
                    INNER JOIN pg_roles r ON r.oid = c.relowner 
                    INNER JOIN pg_depend d ON d.objid = c.oid AND d.refobjsubid > 0 AND d.deptype ='a'
                    INNER JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid	
                    WHERE c.relkind = 'S' AND ns.nspname IN ('${schemas.join("','")}') ${
			core.checkServerCompatibility(serverVersion, 10, 0) ? "AND a.attidentity = ''" : ""
		}  	
                ) s, LATERAL pg_sequence_parameters(s.oid) p`;
	},
	/**
	 *
	 * @param {String} schemaName
	 * @param {String} sequenceName
	 * @param {import("../models/serverVersion")} serverVersion
	 */
	getSequencePrivileges: function (schemaName, sequenceName, serverVersion) {
		return `SELECT s.sequence_schema, s.sequence_name, u.usename, ${
			core.checkServerCompatibility(serverVersion, 10, 0) ? "NULL AS cache_value," : "p.cache_value,"
		}
                HAS_SEQUENCE_PRIVILEGE(u.usename,'"${schemaName}"."${sequenceName}"', 'SELECT') as select,
                HAS_SEQUENCE_PRIVILEGE(u.usename,'"${schemaName}"."${sequenceName}"', 'USAGE') as usage,
                HAS_SEQUENCE_PRIVILEGE(u.usename,'"${schemaName}"."${sequenceName}"', 'UPDATE') as update
                FROM information_schema.sequences s, pg_user u ${
					core.checkServerCompatibility(serverVersion, 10, 0) ? "" : ', "' + schemaName + '"."' + sequenceName + '" p'
				}
                WHERE s.sequence_schema = '${schemaName}' and s.sequence_name='${sequenceName}'`;
	},
};

class CatalogApi {
	/**
	 *
	 * @param {import("pg").Client} client
	 * @param {String[]} schemas
	 */
	static async retrieveSchemas(client, schemas) {
		let result = {};

		const namespaces = await client.query(query.getSchemas(schemas));

		await Promise.all(
			namespaces.rows.map(async (namespace) => {
				result[namespace.nspname] = {
					owner: namespace.owner,
				};
			})
		);

		return result;
	}

	/**
	 *
	 * @param {import("pg").Client} client
	 * @param {import("../models/config")} config
	 */
	static async retrieveTables(client, config) {
		let result = {};

		const tables = await client.query(query.getTables(config.compareOptions.schemaCompare.namespaces));

		await Promise.all(
			tables.rows.map(async (table) => {
				const fullTableName = `"${table.schemaname}"."${table.tablename}"`;
				result[fullTableName] = {
					columns: {},
					constraints: {},
					options: {},
					indexes: {},
					privileges: {},
					owner: table.tableowner,
				};

				const columns = await client.query(query.getTableColumns(fullTableName, client.version));
				columns.rows.forEach((column) => {
					let columnName = `"${column.attname}"`;
					let columnIdentity = null;
					let defaultValue = column.adsrc;
					let dataType = column.typname;

					switch (column.attidentity) {
						case "a":
							columnIdentity = "ALWAYS";
							defaultValue = "";
							break;
						case "d":
							columnIdentity = "BY DEFAULT";
							defaultValue = "";
							break;
						default:
							if (column.adsrc && column.adsrc.startsWith("nextval") && column.adsrc.includes("_seq")) {
								defaultValue = "";
								dataType = "serial";
							}
							break;
					}

					result[fullTableName].columns[columnName] = {
						nullable: !column.attnotnull,
						datatype: dataType,
						dataTypeID: column.typeid,
						dataTypeCategory: column.typcategory,
						default: defaultValue,
						precision: column.precision,
						scale: column.scale,
						identity: columnIdentity,
					};
				});

				let constraints = await client.query(query.getTableConstraints(fullTableName));
				constraints.rows.forEach((constraint) => {
					let constraintName = `"${constraint.conname}"`;
					result[fullTableName].constraints[constraintName] = {
						type: constraint.contype,
						definition: constraint.definition,
					};
				});

				//@mso -> relhadoids has been deprecated from PG v12.0
				if (!core.checkServerCompatibility(client.version, 12, 0)) {
					let options = await client.query(query.getTableOptions(fullTableName));
					options.rows.forEach((option) => {
						result[fullTableName].options = {
							withOids: option.relhasoids,
						};
					});
				}

				let indexes = await client.query(query.getTableIndexes(table.schemaname, table.tablename));
				indexes.rows.forEach((index) => {
					result[fullTableName].indexes[index.indexname] = {
						definition: index.indexdef,
					};
				});

				let privileges = await client.query(query.getTablePrivileges(table.schemaname, table.tablename));
				privileges.rows.forEach((privilege) => {
					if (
						config.compareOptions.schemaCompare.roles.length <= 0 ||
						config.compareOptions.schemaCompare.roles.includes(privilege.usename)
					)
						result[fullTableName].privileges[privilege.usename] = {
							select: privilege.select,
							insert: privilege.insert,
							update: privilege.update,
							delete: privilege.delete,
							truncate: privilege.truncate,
							references: privilege.references,
							trigger: privilege.trigger,
						};
				});

				//TODO: Missing discovering of PARTITION
				//TODO: Missing discovering of TRIGGER
				//TODO: Missing discovering of GRANTS for COLUMNS
				//TODO: Missing discovering of WITH GRANT OPTION, that is used to indicate if user\role can add GRANTS to other users
			})
		);

		return result;
	}

	/**
	 *
	 * @param {import("pg").Client} client
	 * @param {import("../models/config")} config
	 */
	static async retrieveViews(client, config) {
		let result = {};

		//Get views
		const views = await client.query(query.getViews(config.compareOptions.schemaCompare.namespaces));

		await Promise.all(
			views.rows.map(async (view) => {
				const fullViewName = `"${view.schemaname}"."${view.viewname}"`;
				result[fullViewName] = {
					definition: view.definition,
					owner: view.viewowner,
					privileges: {},
					dependencies: [],
				};

				let privileges = await client.query(query.getViewPrivileges(view.schemaname, view.viewname));
				privileges.rows.forEach((privilege) => {
					if (
						config.compareOptions.schemaCompare.roles.length <= 0 ||
						config.compareOptions.schemaCompare.roles.includes(privilege.usename)
					)
						result[fullViewName].privileges[privilege.usename] = {
							select: privilege.select,
							insert: privilege.insert,
							update: privilege.update,
							delete: privilege.delete,
							truncate: privilege.truncate,
							references: privilege.references,
							trigger: privilege.trigger,
						};
				});

				let dependencies = await client.query(query.getViewDependencies(view.schemaname, view.viewname));
				dependencies.rows.forEach((dependency) => {
					result[fullViewName].dependencies.push({
						schemaName: dependency.schemaname,
						tableName: dependency.tablename,
						columnName: dependency.columnname,
					});
				});
			})
		);

		//TODO: Missing discovering of TRIGGER
		//TODO: Missing discovering of GRANTS for COLUMNS
		//TODO: Should we get TEMPORARY VIEW?

		return result;
	}

	/**
	 *
	 * @param {import("pg").Client} client
	 * @param {import("../models/config")} config
	 */
	static async retrieveMaterializedViews(client, config) {
		let result = {};

		const views = await client.query(query.getMaterializedViews(config.compareOptions.schemaCompare.namespaces));

		await Promise.all(
			views.rows.map(async (view) => {
				const fullViewName = `"${view.schemaname}"."${view.matviewname}"`;
				result[fullViewName] = {
					definition: view.definition,
					indexes: {},
					owner: view.matviewowner,
					privileges: {},
					dependencies: [],
				};

				let indexes = await client.query(query.getTableIndexes(view.schemaname, view.matviewname));
				indexes.rows.forEach((index) => {
					result[fullViewName].indexes[index.indexname] = {
						definition: index.indexdef,
					};
				});

				let privileges = await client.query(query.getMaterializedViewPrivileges(view.schemaname, view.matviewname));
				privileges.rows.forEach((privilege) => {
					if (
						config.compareOptions.schemaCompare.roles.length <= 0 ||
						config.compareOptions.schemaCompare.roles.includes(privilege.usename)
					)
						result[fullViewName].privileges[privilege.usename] = {
							select: privilege.select,
							insert: privilege.insert,
							update: privilege.update,
							delete: privilege.delete,
							truncate: privilege.truncate,
							references: privilege.references,
							trigger: privilege.trigger,
						};
				});

				let dependencies = await client.query(query.getViewDependencies(view.schemaname, view.matviewname));
				dependencies.rows.forEach((dependency) => {
					result[fullViewName].dependencies.push({
						schemaName: dependency.schemaname,
						tableName: dependency.tablename,
						columnName: dependency.columnname,
					});
				});
			})
		);

		//TODO: Missing discovering of GRANTS for COLUMNS

		return result;
	}

	/**
	 *
	 * @param {import("pg").Client} client
	 * @param {import("../models/config")} config
	 */
	static async retrieveFunctions(client, config) {
		let result = {};

		const procedures = await client.query(query.getFunctions(config.compareOptions.schemaCompare.namespaces, client.version));

		await Promise.all(
			procedures.rows.map(async (procedure) => {
				let fullProcedureName = `"${procedure.nspname}"."${procedure.proname}"`;
				result[fullProcedureName] = {
					definition: procedure.definition,
					owner: procedure.owner,
					argTypes: procedure.argtypes,
					privileges: {},
				};

				let privileges = await client.query(query.getFunctionPrivileges(procedure.nspname, procedure.proname, procedure.argtypes));

				privileges.rows.forEach((privilege) => {
					if (
						config.compareOptions.schemaCompare.roles.length <= 0 ||
						config.compareOptions.schemaCompare.roles.includes(privilege.usename)
					)
						result[fullProcedureName].privileges[privilege.usename] = {
							execute: privilege.execute,
						};
				});
			})
		);

		return result;
	}

	/**
	 *
	 * @param {import("pg").Client} client
	 * @param {import("../models/config")} config
	 */
	static async retrieveSequences(client, config) {
		let result = {};

		const sequences = await client.query(query.getSequences(config.compareOptions.schemaCompare.namespaces, client.version));

		await Promise.all(
			sequences.rows.map(async (sequence) => {
				let fullSequenceName = `"${sequence.seq_nspname}"."${sequence.seq_name}"`;
				result[fullSequenceName] = {
					owner: sequence.owner,
					startValue: sequence.start_value,
					minValue: sequence.minimum_value,
					maxValue: sequence.maximum_value,
					increment: sequence.increment,
					cacheSize: sequence.cache_size,
					isCycle: sequence.cycle_option,
					name: sequence.seq_name,
					ownedBy: `${sequence.ownedby_table}.${sequence.ownedby_column}`,
					privileges: {},
				};

				let privileges = await client.query(query.getSequencePrivileges(sequence.seq_nspname, sequence.seq_name, client.version));

				privileges.rows.forEach((privilege) => {
					if (privilege.cache_value != null) result[fullSequenceName].cacheSize = privilege.cache_value;

					if (
						config.compareOptions.schemaCompare.roles.length <= 0 ||
						config.compareOptions.schemaCompare.roles.includes(privilege.usename)
					)
						result[fullSequenceName].privileges[privilege.usename] = {
							select: privilege.select,
							usage: privilege.usage,
							update: privilege.update,
						};
				});
			})
		);
		return result;
	}
}

module.exports = CatalogApi;
