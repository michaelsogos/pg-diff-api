class DatabaseObjects {
    constructor() {
        /** @type {Object} The definition of schemas*/
        this.schemas = null;
        /** @type {Object} The definition of tables*/
        this.tables = null;
        /** @type {Object} The definition of views*/
        this.views = null;
        /** @type {Object} The definition of materialized views*/
        this.materializedViews = null;
        /** @type {Object} The definition of functions*/
        this.functions = null;
        /** @type {Object} The definition of sequences*/
        this.sequences = null;
    }
}

module.exports = DatabaseObjects;
