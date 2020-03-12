/*jslint node: true */
/*jshint esversion: 6 */
'use strict';

const crypto = require('crypto');
const USE_LARGE_SCAN = process.env.USE_LARGE_SCAN === 'ON';

module.exports = {
    /**
     * Performs scan for database. Results are sent to resolve function
     *
     * @param {Object} dynamoDb db object
     * @param {Object} params of
     * @param {Promise.resolve} resolve called on success
     * @param {Promise.reject} reject called on failure
     */
    performScan(dynamoDb, params) {
        return new Promise((resolve, reject) => {
            function chatScan(err, data) {
                if (err) {
                    console.log("error while scanning");
                    console.log(err);
                    reject(err);
                    return;
                } else if (!data) {
                    console.log("no data, no error");
                    resolve(allResults);
                    return;
                } else {
                    for (const item of data.Items) {
                        allResults.push(item);
                    }
                }

                // continue scanning if we have more, because
                // scan can retrieve a maximum of 1MB of data
                if (typeof data.LastEvaluatedKey != "undefined" && USE_LARGE_SCAN) {
                    console.log("Scanning for more...");
                    params.ExclusiveStartKey = data.LastEvaluatedKey;
                    dynamoDb.scan(params, chatScan);
                } else {
                    resolve(allResults);
                }
            }

            var allResults = [];

            dynamoDb.scan(params, chatScan);
        });
    },
    createHash(source) {
        var hash = crypto.createHash('sha1');
        hash.update(source);
        return hash.digest('hex');
    }
};