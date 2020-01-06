class TableDefinition {
    /**
     *
     * @param {String} tableName
     * @param {String[]} tableKeyFields
     * @param {String} tableSchema
     */
    constructor(tableName, tableKeyFields, tableSchema) {
        /** @type {String} */
        this.tableName = tableName;
        /** @type {String} */
        this.tableSchema = tableSchema;
        /** @type {String[]} */
        this.tableKeyFields = tableKeyFields;
    }
}

module.exports = TableDefinition;
