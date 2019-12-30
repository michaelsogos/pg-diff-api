class ServerVersion {
    constructor(major, minor, patch, version) {
        /** @type {number} */
        this.major = major || 0;
        /** @type {number} */
        this.minor = minor || 0;
        /** @type {number} */
        this.patch = patch || 0;
        /** @type {String} */
        this.value = version;
    }
}

module.exports = ServerVersion;
