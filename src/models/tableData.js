class TableData {
    constructor() {
        this.sourceData = {
            records: {
                fields: [],
                rows: [],
            },
            sequences: [],
        };

        this.targetData = {
            records: {
                fields: [],
                rows: [],
            },
            sequences: [],
        };
    }
}

module.exports = TableData;
