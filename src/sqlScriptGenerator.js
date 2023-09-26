const objectType = require("./enums/objectType");

const hints = {
	addColumnNotNullableWithoutDefaultValue:
		" --WARN: Add a new column not nullable without a default value can occure in a sql error during execution!",
	changeColumnDataType:
		" --WARN: Change column data type can occure in a casting error, the suggested casting expression is the default one and may not fit your needs!",
	dropColumn: " --WARN: Drop column can occure in data loss!",
	potentialRoleMissing:
		" --WARN: Grant\\Revoke privileges to a role can occure in a sql error during execution if role is missing to the target database!",
	identityColumnDetected: " --WARN: Identity column has been detected, an error can occure because constraints violation!",
	dropTable: " --WARN: Drop table can occure in data loss!",
};

var helper = {
	/**
	 *
	 * @param {Object} columnSchema
	 */
	__generateColumnDataTypeDefinition: function (columnSchema) {
		let dataType = columnSchema.datatype;
		if (columnSchema.precision) {
			let dataTypeScale = columnSchema.scale ? `,${columnSchema.scale}` : "";
			dataType += `(${columnSchema.precision}${dataTypeScale})`;
		}

		return dataType;
	},
	/**
	 *
	 * @param {String} column
	 * @param {Object} columnSchema
	 */
	__generateColumnDefinition: function (column, columnSchema) {
		let nullableExpression = columnSchema.nullable ? "NULL" : "NOT NULL";

		let defaultValue = "";
		if (columnSchema.default) defaultValue = `DEFAULT ${columnSchema.default}`;

		let identityValue = "";
		if (columnSchema.identity) identityValue = `GENERATED ${columnSchema.identity} AS IDENTITY`;

		if (columnSchema.generatedColumn) {
			nullableExpression = "";
			defaultValue = `GENERATED ALWAYS AS ${columnSchema.default} STORED`;
			identityValue = "";
		}

		let dataType = this.__generateColumnDataTypeDefinition(columnSchema);

		return `${column} ${dataType} ${nullableExpression} ${defaultValue} ${identityValue}`;
	},
	/**
	 *
	 * @param {String} table
	 * @param {String} role
	 * @param {Object} privileges
	 */
	__generateTableGrantsDefinition: function (table, role, privileges) {
		let definitions = [];

		if (privileges.select) definitions.push(`GRANT SELECT ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`);

		if (privileges.insert) definitions.push(`GRANT INSERT ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`);

		if (privileges.update) definitions.push(`GRANT UPDATE ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`);

		if (privileges.delete) definitions.push(`GRANT DELETE ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`);

		if (privileges.truncate) definitions.push(`GRANT TRUNCATE ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`);

		if (privileges.references) definitions.push(`GRANT REFERENCES ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`);

		if (privileges.trigger) definitions.push(`GRANT TRIGGER ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`);

		return definitions;
	},
	/**
	 *
	 * @param {String} procedure
	 * @param {String} argTypes
	 * @param {String} role
	 * @param {Object} privileges
	 * @param {"f"|"p"} type
	 */
	__generateProcedureGrantsDefinition: function (procedure, argTypes, role, privileges, type) {
		const procedureType = type === "f" ? "FUNCTION" : "PROCEDURE";

		let definitions = [];

		if (privileges.execute)
			definitions.push(`GRANT EXECUTE ON ${procedureType} ${procedure}(${argTypes}) TO ${role};${hints.potentialRoleMissing}`);

		return definitions;
	},
	/**
	 *
	 * @param {String} sequence
	 * @param {String} role
	 * @param {Object} privileges
	 */
	__generateSequenceGrantsDefinition: function (sequence, role, privileges) {
		let definitions = [];

		if (privileges.select) definitions.push(`GRANT SELECT ON SEQUENCE ${sequence} TO ${role};${hints.potentialRoleMissing}`);

		if (privileges.usage) definitions.push(`GRANT USAGE ON SEQUENCE ${sequence} TO ${role};${hints.potentialRoleMissing}`);

		if (privileges.update) definitions.push(`GRANT UPDATE ON SEQUENCE ${sequence} TO ${role};${hints.potentialRoleMissing}`);

		return definitions;
	},
	/**
	 *
	 * @param {String} objectType
	 * @param {String} objectName
	 * @param {String} comment
	 * @param {String} parentObjectName
	 */
	generateChangeCommentScript: function (objectType, objectName, comment, parentObjectName = null) {
		const description = comment ? `'${comment.replace("'", "''")}'` : "NULL";
		const parentObject = parentObjectName ? `ON ${parentObjectName}` : "";
		let script = `\nCOMMENT ON ${objectType} ${objectName} ${parentObject} IS ${description};\n`;
		return script;
	},
	/**
	 *
	 * @param {String} schema
	 * @param {String} owner
	 */
	generateCreateSchemaScript: function (schema, owner) {
		let script = `\nCREATE SCHEMA IF NOT EXISTS ${schema} AUTHORIZATION ${owner};\n`;
		return script;
	},
	/**
	 *
	 * @param {String} table
	 */
	generateDropTableScript: function (table) {
		let script = `\nDROP TABLE IF EXISTS ${table};\n`;
		return script;
	},
	/**
	 *
	 * @param {String} table
	 * @param {Object} schema
	 */
	generateCreateTableScript: function (table, schema) {
		//Generate columns script
		let columns = [];
		for (let column in schema.columns) {
			columns.push(this.__generateColumnDefinition(column, schema.columns[column]));
		}

		//Generate constraints script
		for (let constraint in schema.constraints) {
			columns.push(`CONSTRAINT ${constraint} ${schema.constraints[constraint].definition} `);
		}

		//Generate options script
		let options = "";
		if (schema.options && schema.options.withOids) options = `\nWITH ( OIDS=${schema.options.withOids.toString().toUpperCase()} )`;

		//Generate indexes script
		let indexes = [];
		for (let index in schema.indexes) {
			let definition = schema.indexes[index].definition;
			definition = definition.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS");
			definition = definition.replace("CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX IF NOT EXISTS");

			indexes.push(`\n${definition};\n`);
		}

		//Generate privileges script
		let privileges = [];
		privileges.push(`ALTER TABLE IF EXISTS ${table} OWNER TO ${schema.owner};\n`);
		for (let role in schema.privileges) {
			privileges = privileges.concat(this.__generateTableGrantsDefinition(table, role, schema.privileges[role]));
		}

		let columnsComment = [];
		for (let column in schema.columns) {
			columnsComment.push(this.generateChangeCommentScript(objectType.COLUMN, `${table}.${column}`, schema.columns[column].comment));
		}

		let constraintsComment = [];
		for (let constraint in schema.constraints) {
			constraintsComment.push(
				this.generateChangeCommentScript(objectType.CONSTRAINT, constraint, schema.constraints[constraint].comment, table)
			);
		}

		let indexesComment = [];
		for (let index in schema.indexes) {
			indexesComment.push(
				this.generateChangeCommentScript(objectType.INDEX, `"${schema.indexes[index].schema}"."${index}"`, schema.indexes[index].comment)
			);
		}

		let script = `\nCREATE TABLE IF NOT EXISTS ${table} (\n\t${columns.join(",\n\t")}\n)${options};\n${indexes.join("\n")}\n${privileges.join(
			"\n"
		)}\n${columnsComment.join("\n")}${constraintsComment.join("\n")}${indexesComment.join("\n")}`;

		return script;
	},
	/**
	 *
	 * @param {String} table
	 * @param {String} column
	 * @param {Object} schema
	 */
	generateAddTableColumnScript: function (table, column, schema) {
		let script = `\nALTER TABLE IF EXISTS ${table} ADD COLUMN IF NOT EXISTS ${this.__generateColumnDefinition(column, schema)};`;
		if (script.includes("NOT NULL") && !script.includes("DEFAULT")) script += hints.addColumnNotNullableWithoutDefaultValue;

		script += "\n";
		return script;
	},
	/**
	 *
	 * @param {String} table
	 * @param {String} column
	 * @param {Object} changes
	 */
	generateChangeTableColumnScript: function (table, column, changes) {
		let definitions = [];
		if (Object.prototype.hasOwnProperty.call(changes, "nullable"))
			definitions.push(`ALTER COLUMN ${column} ${changes.nullable ? "DROP NOT NULL" : "SET NOT NULL"}`);

		if (changes.datatype) {
			definitions.push(`${hints.changeColumnDataType}`);
			let dataTypeDefinition = this.__generateColumnDataTypeDefinition(changes);
			definitions.push(`ALTER COLUMN ${column} SET DATA TYPE ${dataTypeDefinition} USING ${column}::${dataTypeDefinition}`);
		}

		if (Object.prototype.hasOwnProperty.call(changes, "default"))
			definitions.push(`ALTER COLUMN ${column} ${changes.default ? "SET" : "DROP"} DEFAULT ${changes.default || ""}`);

		if (Object.prototype.hasOwnProperty.call(changes, "identity") && Object.prototype.hasOwnProperty.call(changes, "isNewIdentity")) {
			let identityDefinition = "";
			if (changes.identity) {
				//truly values
				identityDefinition = `${changes.isNewIdentity ? "ADD" : "SET"} GENERATED ${changes.identity} ${
					changes.isNewIdentity ? "AS IDENTITY" : ""
				}`;
			} else {
				//falsy values
				identityDefinition = "DROP IDENTITY IF EXISTS";
			}
			definitions.push(`ALTER COLUMN ${column} ${identityDefinition}`);
		}

		let script = `\nALTER TABLE IF EXISTS ${table}\n\t${definitions.join(",\n\t")};\n`;

		//TODO: Should we include COLLATE when change column data type?

		return script;
	},
	/**
	 *
	 * @param {String} table
	 * @param {String} column
	 */
	generateDropTableColumnScript: function (table, column, withoutHint = false) {
		let script = `\nALTER TABLE IF EXISTS ${table} DROP COLUMN IF EXISTS ${column} CASCADE;${withoutHint ? "" : hints.dropColumn}\n`;
		return script;
	},
	/**
	 *
	 * @param {String} table
	 * @param {String} constraint
	 * @param {Object} schema
	 */
	generateAddTableConstraintScript: function (table, constraint, schema) {
		let script = `\nALTER TABLE IF EXISTS ${table} ADD CONSTRAINT ${constraint} ${schema.definition};\n`;
		return script;
	},
	/**
	 *
	 * @param {String} table
	 * @param {String} constraint
	 */
	generateDropTableConstraintScript: function (table, constraint) {
		let script = `\nALTER TABLE IF EXISTS ${table} DROP CONSTRAINT IF EXISTS ${constraint};\n`;
		return script;
	},
	/**
	 *
	 * @param {String} table
	 * @param {Object} options
	 */
	generateChangeTableOptionsScript: function (table, options) {
		let script = `\nALTER TABLE IF EXISTS ${table} SET ${options.withOids ? "WITH" : "WITHOUT"} OIDS;\n`;
		return script;
	},
	generateChangeIndexScript: function (index, definition) {
		let script = `\nDROP INDEX IF EXISTS ${index};\n${definition};\n`;
		return script;
	},
	/**
	 *
	 * @param {String} index
	 */
	generateDropIndexScript: function (index) {
		let script = `\nDROP INDEX IF EXISTS ${index};\n`;
		return script;
	},
	/**
	 *
	 * @param {String} table
	 * @param {String} role
	 * @param {Object} privileges
	 */
	generateTableRoleGrantsScript: function (table, role, privileges) {
		let script = `\n${this.__generateTableGrantsDefinition(table, role, privileges).join("\n")}\n`;
		return script;
	},
	/**
	 *
	 * @param {String} table
	 * @param {String} role
	 * @param {Object} changes
	 */
	generateChangesTableRoleGrantsScript: function (table, role, changes) {
		let privileges = [];

		if (Object.prototype.hasOwnProperty.call(changes, "select"))
			privileges.push(
				`${changes.select ? "GRANT" : "REVOKE"} SELECT ON TABLE ${table} ${changes.select ? "TO" : "FROM"} ${role};${
					hints.potentialRoleMissing
				}`
			);

		if (Object.prototype.hasOwnProperty.call(changes, "insert"))
			privileges.push(
				`${changes.insert ? "GRANT" : "REVOKE"} INSERT ON TABLE ${table} ${changes.insert ? "TO" : "FROM"} ${role};${
					hints.potentialRoleMissing
				}`
			);

		if (Object.prototype.hasOwnProperty.call(changes, "update"))
			privileges.push(
				`${changes.update ? "GRANT" : "REVOKE"} UPDATE ON TABLE ${table} ${changes.update ? "TO" : "FROM"} ${role};${
					hints.potentialRoleMissing
				}`
			);

		if (Object.prototype.hasOwnProperty.call(changes, "delete"))
			privileges.push(
				`${changes.delete ? "GRANT" : "REVOKE"} DELETE ON TABLE ${table} ${changes.delete ? "TO" : "FROM"} ${role};${
					hints.potentialRoleMissing
				}`
			);

		if (Object.prototype.hasOwnProperty.call(changes, "truncate"))
			privileges.push(
				`${changes.truncate ? "GRANT" : "REVOKE"} TRUNCATE ON TABLE ${table} ${changes.truncate ? "TO" : "FROM"} ${role};${
					hints.potentialRoleMissing
				}`
			);

		if (Object.prototype.hasOwnProperty.call(changes, "references"))
			privileges.push(
				`${changes.references ? "GRANT" : "REVOKE"} REFERENCES ON TABLE ${table} ${changes.references ? "TO" : "FROM"} ${role};${
					hints.potentialRoleMissing
				}`
			);

		if (Object.prototype.hasOwnProperty.call(changes, "trigger"))
			privileges.push(
				`${changes.trigger ? "GRANT" : "REVOKE"} TRIGGER ON TABLE ${table} ${changes.trigger ? "TO" : "FROM"} ${role};${
					hints.potentialRoleMissing
				}`
			);

		let script = `\n${privileges.join("\n")}\n`;
		return script;
	},
	/**
	 *
	 * @param {String} table
	 * @param {String} owner
	 */
	generateChangeTableOwnerScript: function (table, owner) {
		let script = `\nALTER TABLE IF EXISTS ${table} OWNER TO ${owner};\n`;
		return script;
	},
	/**
	 *
	 * @param {String} view
	 * @param {Object} schema
	 */
	generateCreateViewScript: function (view, schema) {
		//Generate privileges script
		let privileges = [];
		privileges.push(`ALTER VIEW IF EXISTS ${view} OWNER TO ${schema.owner};`);
		for (let role in schema.privileges) {
			privileges = privileges.concat(this.__generateTableGrantsDefinition(view, role, schema.privileges[role]));
		}

		let script = `\nCREATE OR REPLACE VIEW ${view} AS ${schema.definition}\n${privileges.join("\n")}\n`;
		return script;
	},
	/**
	 *
	 * @param {String} view
	 */
	generateDropViewScript: function (view) {
		let script = `\nDROP VIEW IF EXISTS ${view};`;
		return script;
	},
	/**
	 *
	 * @param {String} view
	 * @param {Object} schema
	 */
	generateCreateMaterializedViewScript: function (view, schema) {
		//Generate indexes script
		let indexes = [];
		for (let index in schema.indexes) {
			indexes.push(`\n${schema.indexes[index].definition};\n`);
		}

		//Generate privileges script
		let privileges = [];
		privileges.push(`ALTER MATERIALIZED VIEW IF EXISTS ${view} OWNER TO ${schema.owner};\n`);
		for (let role in schema.privileges) {
			privileges = privileges.concat(this.__generateTableGrantsDefinition(view, role, schema.privileges[role]));
		}

		let script = `\nCREATE MATERIALIZED VIEW IF NOT EXISTS ${view} AS ${schema.definition}\n${indexes.join("\n")}\n${privileges.join("\n")}\n`;
		return script;
	},
	/**
	 *
	 * @param {String} view
	 */
	generateDropMaterializedViewScript: function (view) {
		let script = `\nDROP MATERIALIZED VIEW IF EXISTS ${view};`;
		return script;
	},
	/**
	 *
	 * @param {String} procedure
	 * @param {Object} schema
	 * @param {"f"|"p"} type
	 */
	generateCreateProcedureScript: function (procedure, schema) {
		const procedureType = schema.type === "f" ? "FUNCTION" : "PROCEDURE";

		//Generate privileges script
		let privileges = [];
		privileges.push(`ALTER ${procedureType} ${procedure}(${schema.argTypes}) OWNER TO ${schema.owner};`);
		for (let role in schema.privileges) {
			privileges = privileges.concat(
				this.__generateProcedureGrantsDefinition(procedure, schema.argTypes, role, schema.privileges[role], schema.type)
			);
		}

		let script = `\n${schema.definition};\n${privileges.join("\n")}\n`;
		return script;
	},
	/**
	 *
	 * @param {String} aggregate
	 * @param {Object} schema
	 */
	generateCreateAggregateScript: function (aggregate, schema) {
		//Generate privileges script
		let privileges = [];
		privileges.push(`ALTER AGGREGATE ${aggregate}(${schema.argTypes}) OWNER TO ${schema.owner};`);
		for (let role in schema.privileges) {
			privileges = privileges.concat(this.__generateProcedureGrantsDefinition(aggregate, schema.argTypes, role, schema.privileges[role], "f"));
		}

		let script = `\nCREATE AGGREGATE ${aggregate} (${schema.argTypes}) (\n${schema.definition}\n);\n${privileges.join("\n")}\n`;
		return script;
	},
	/**
	 *
	 * @param {String} procedure
	 * @param {Object} schema
	 */
	generateChangeProcedureScript: function (procedure, schema) {
		const procedureType = schema.type === "f" ? "FUNCTION" : "PROCEDURE";

		let script = `\nDROP ${procedureType} IF EXISTS ${procedure}(${schema.argTypes});\n${this.generateCreateProcedureScript(procedure, schema)}`;
		return script;
	},
	/**
	 *
	 * @param {String} aggregate
	 * @param {Object} schema
	 */
	generateChangeAggregateScript: function (aggregate, schema) {
		let script = `\nDROP AGGREGATE IF EXISTS ${aggregate}(${schema.argTypes});\n${this.generateCreateAggregateScript(aggregate, schema)}`;
		return script;
	},
	/**
	 *
	 * @param {String} procedure
	 * @param {String} procedureArgs
	 */
	generateDropProcedureScript: function (procedure, procedureArgs) {
		let script = `\nDROP FUNCTION IF EXISTS ${procedure}(${procedureArgs});\n`;
		return script;
	},
	/**
	 *
	 * @param {String} aggregate
	 * @param {String} aggregateArgs
	 */
	generateDropAggregateScript: function (aggregate, aggregateArgs) {
		let script = `\nDROP AGGREGATE IF EXISTS ${aggregate}(${aggregateArgs});\n`;
		return script;
	},
	/**
	 *
	 * @param {String} procedure
	 * @param {String} argTypes
	 * @param {String} role
	 * @param {Object} privileges
	 * @param {"f"|"p"} type
	 */
	generateProcedureRoleGrantsScript: function (procedure, argTypes, role, privileges, type) {
		let script = `\n${this.__generateProcedureGrantsDefinition(procedure, argTypes, role, privileges, type).join("\n")}`;
		return script;
	},
	/**
	 *
	 * @param {String} procedure
	 * @param {String} argTypes
	 * @param {String} role
	 * @param {Object} changes
	 * @param {"f"|"p"} type
	 */
	generateChangesProcedureRoleGrantsScript: function (procedure, argTypes, role, changes, type) {
		const procedureType = type === "f" ? "FUNCTION" : "PROCEDURE";
		let privileges = [];

		if (Object.prototype.hasOwnProperty.call(changes, "execute"))
			privileges.push(
				`${changes.execute ? "GRANT" : "REVOKE"} EXECUTE ON ${procedureType} ${procedure}(${argTypes}) ${
					changes.execute ? "TO" : "FROM"
				} ${role};${hints.potentialRoleMissing}`
			);

		let script = `\n${privileges.join("\n")}`;
		return script;
	},
	/**
	 *
	 * @param {String} procedure
	 * @param {String} argTypes
	 * @param {String} owner
	 * @param {"p"!"f"} type
	 */
	generateChangeProcedureOwnerScript: function (procedure, argTypes, owner, type) {
		const procedureType = type === "f" ? "FUNCTION" : "PROCEDURE";

		let script = `\nALTER ${procedureType} ${procedure}(${argTypes}) OWNER TO ${owner};`;
		return script;
	},
	/**
	 *
	 * @param {String} aggregate
	 * @param {String} argTypes
	 * @param {String} owner
	 */
	generateChangeAggregateOwnerScript: function (aggregate, argTypes, owner) {
		let script = `\nALTER AGGREGATE ${aggregate}(${argTypes}) OWNER TO ${owner};`;
		return script;
	},
	/**
	 *
	 * @param {String} table
	 * @param {Object} fields
	 * @param {Object} filterConditions
	 * @param {Object} changes
	 */
	generateUpdateTableRecordScript: function (table, fields, filterConditions, changes) {
		let updates = [];
		for (let field in changes) {
			updates.push(`"${field}" = ${this.__generateSqlFormattedValue(field, fields, changes[field])}`);
		}

		let conditions = [];
		for (let condition in filterConditions) {
			conditions.push(`"${condition}" = ${this.__generateSqlFormattedValue(condition, fields, filterConditions[condition])}`);
		}

		let script = `\nUPDATE ${table} SET ${updates.join(", ")} WHERE ${conditions.join(" AND ")};\n`;
		return script;
	},
	/**
	 *
	 * @param {String} table
	 * @param {Object} record
	 * @param {Array} fields
	 * @param {Boolean} isIdentityValuesAllowed
	 */
	generateInsertTableRecordScript: function (table, record, fields, isIdentityValuesAllowed) {
		let fieldNames = [];
		let fieldValues = [];
		for (let field in record) {
			fieldNames.push(`"${field}"`);
			fieldValues.push(this.__generateSqlFormattedValue(field, fields, record[field]));
		}

		let script = `\nINSERT INTO ${table} (${fieldNames.join(", ")}) ${
			isIdentityValuesAllowed ? "" : "OVERRIDING SYSTEM VALUE"
		} VALUES (${fieldValues.join(", ")});\n`;
		if (!isIdentityValuesAllowed) script = `\n${hints.identityColumnDetected}` + script;
		return script;
	},
	/**
	 *
	 * @param {String} table
	 * @param {Array} fields
	 * @param {Object} keyFieldsMap
	 */
	generateDeleteTableRecordScript: function (table, fields, keyFieldsMap) {
		let conditions = [];
		for (let condition in keyFieldsMap) {
			conditions.push(`"${condition}" = ${this.__generateSqlFormattedValue(condition, fields, keyFieldsMap[condition])}`);
		}

		let script = `\nDELETE FROM ${table} WHERE ${conditions.join(" AND ")};\n`;
		return script;
	},
	/**
	 *
	 * @param {String} fieldName
	 * @param {Array} fields
	 * @param {Object} value
	 */
	__generateSqlFormattedValue: function (fieldName, fields, value) {
		if (value === undefined) throw new Error(`The field "${fieldName}" contains an "undefined" value!`);
		if (value === null) return "NULL";

		let dataTypeName = "";
		let dataTypeCategory = "X";

		let dataTypeIndex = fields.findIndex((field) => {
			return fieldName === field.name;
		});

		if (dataTypeIndex >= 0) {
			dataTypeName = fields[dataTypeIndex].datatype;
			dataTypeCategory = fields[dataTypeIndex].dataTypeCategory;
			if (fields[dataTypeIndex].isGeneratedColumn) return "DEFAULT";
		}

		switch (dataTypeCategory) {
			case "D": //DATE TIME
				return `'${value.toISOString()}'`;
			case "V": //BIT
			case "S": //STRING
				return `'${value.replace(/'/g, "''")}'`;
			// return `'${value}'`;
			case "A": //ARRAY
				return `'{${value.join()}}'`;
			case "R": //RANGE
				return `'${value}'`;
			case "B": //BOOL
			case "E": //ENUM
			case "G": //GEOMETRIC
			case "I": //NETWORK ADDRESS
			case "N": //NUMERIC
			case "T": //TIMESPAN
				return value;
			case "U": {
				//USER TYPE
				switch (dataTypeName) {
					case "jsonb":
					case "json":
						return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
					default:
						//like XML, UUID, GEOMETRY, etc.
						return `'${value.replace(/'/g, "''")}'`;
				}
			}
			case "X": //UNKNOWN
			case "P": //PSEUDO TYPE
			case "C": //COMPOSITE TYPE
			default:
				throw new Error(`The data type category '${dataTypeCategory}' is not implemented yet!`);
		}
	},
	generateMergeTableRecord(table, fields, changes, options) {
		let fieldNames = [];
		let fieldValues = [];
		let updates = [];
		for (let field in changes) {
			fieldNames.push(`"${field}"`);
			fieldValues.push(this.__generateSqlFormattedValue(field, fields, changes[field]));
			updates.push(`"${field}" = ${this.__generateSqlFormattedValue(field, fields, changes[field])}`);
		}

		let conflictDefinition = "";
		if (options.constraintName) conflictDefinition = `ON CONSTRAINT ${options.constraintName}`;
		else if (options.uniqueFields && options.uniqueFields.length > 0) conflictDefinition = `("${options.uniqueFields.join('", "')}")`;
		else throw new Error(`Impossible to generate conflict definition for table ${table} record to merge!`);

		let script = `\nINSERT INTO ${table} (${fieldNames.join(", ")}) VALUES (${fieldValues.join(
			", "
		)})\nON CONFLICT ${conflictDefinition}\nDO UPDATE SET ${updates.join(", ")}`;
		return script;
	},
	/**
	 *
	 * @param {String} tableName
	 * @param {Object} sequence
	 */
	generateSetSequenceValueScript(tableName, sequence) {
		let script = `\nSELECT setval(pg_get_serial_sequence('${tableName}', '${sequence.attname}'), max("${sequence.attname}"), true) FROM ${tableName};\n`;
		return script;
	},
	/**
	 *
	 * @param {String} sequence
	 * @param {String} property
	 * @param {Number} value
	 */
	generateChangeSequencePropertyScript(sequence, property, value) {
		var definition = "";
		switch (property) {
			case "startValue":
				definition = `START WITH ${value}`;
				break;
			case "minValue":
				definition = `MINVALUE ${value}`;
				break;
			case "maxValue":
				definition = `MAXVALUE ${value}`;
				break;
			case "increment":
				definition = `INCREMENT BY ${value}`;
				break;
			case "cacheSize":
				definition = `CACHE ${value}`;
				break;
			case "isCycle":
				definition = `${value ? "" : "NO"} CYCLE`;
				break;
			case "owner":
				definition = `OWNER TO ${value}`;
				break;
		}

		let script = `\nALTER SEQUENCE IF EXISTS ${sequence} ${definition};\n`;
		return script;
	},
	/**
	 *
	 * @param {String} sequence
	 * @param {String} role
	 * @param {Object} changes
	 */
	generateChangesSequenceRoleGrantsScript: function (sequence, role, changes) {
		let privileges = [];

		if (Object.prototype.hasOwnProperty.call(changes, "select"))
			privileges.push(
				`${changes.select ? "GRANT" : "REVOKE"} SELECT ON SEQUENCE ${sequence} ${changes.select ? "TO" : "FROM"} ${role};${
					hints.potentialRoleMissing
				}`
			);

		if (Object.prototype.hasOwnProperty.call(changes, "usage"))
			privileges.push(
				`${changes.usage ? "GRANT" : "REVOKE"} USAGE ON SEQUENCE ${sequence} ${changes.usage ? "TO" : "FROM"} ${role};${
					hints.potentialRoleMissing
				}`
			);

		if (Object.prototype.hasOwnProperty.call(changes, "update"))
			privileges.push(
				`${changes.update ? "GRANT" : "REVOKE"} UPDATE ON SEQUENCE ${sequence} ${changes.update ? "TO" : "FROM"} ${role};${
					hints.potentialRoleMissing
				}`
			);

		let script = `\n${privileges.join("\n")}`;

		return script;
	},
	/**
	 *
	 * @param {String} sequence
	 * @param {String} role
	 * @param {Object} privileges
	 */
	generateSequenceRoleGrantsScript: function (sequence, role, privileges) {
		let script = `\n${this.__generateSequenceGrantsDefinition(sequence, role, privileges).join("\n")}`;
		return script;
	},
	/**
	 *
	 * @param {String} sequence
	 * @param {Object} schema
	 */
	generateCreateSequenceScript: function (sequence, schema) {
		//Generate privileges script
		let privileges = [];
		privileges.push(`ALTER SEQUENCE ${sequence} OWNER TO ${schema.owner};`);
		for (let role in schema.privileges) {
			privileges = privileges.concat(this.__generateSequenceGrantsDefinition(sequence, role, schema.privileges[role]));
		}

		let script = `\n
CREATE SEQUENCE IF NOT EXISTS ${sequence} 
\tINCREMENT BY ${schema.increment} 
\tMINVALUE ${schema.minValue}
\tMAXVALUE ${schema.maxValue}
\tSTART WITH ${schema.startValue}
\tCACHE ${schema.cacheSize}
\t${schema.isCycle ? "" : "NO "}CYCLE;
\n${privileges.join("\n")}\n`;

		return script;
	},
	/**
	 *
	 * @param {String} old_name
	 * @param {String} new_name
	 */
	generateRenameSequenceScript: function (old_name, new_name) {
		let script = `\nALTER SEQUENCE IF EXISTS ${old_name} RENAME TO ${new_name};\n`;
		return script;
	},
};

module.exports = helper;
