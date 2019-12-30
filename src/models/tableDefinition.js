class TableDefinition {
    /**
     *
     * @param {String} tableName
     * @param {String} tableSchema
     * @param {String[]} tableKeyFields
     */
    constructor(tableName, tableSchema, tableKeyFields) {
        /** @type {String} */
        this.tableName = tableName;
        /** @type {String} */
        this.tableSchema = tableSchema;
        /** @type {String[]} */
        this.tableKeyFields = tableKeyFields;
    }
}

module.exports = TableDefinition;
