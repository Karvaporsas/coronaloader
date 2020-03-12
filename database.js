/*jslint node: true */
/*jshint esversion: 6 */
'use strict';

const AWS = require('aws-sdk');
const utils = require('./utils');
const OPERATIONS_TABLE = process.env.TABLE_OPERATIONS;
const DEBUG_MODE = process.env.DEBUG_MODE === 'ON';
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const CONFIRMED_TABLE = process.env.CONFIRMED_TABLE;
const DEATHS_TABLE = process.env.DEATHS_TABLE;
const RECOVERED_TABLE = process.env.RECOVERED_TABLE;
const corona_info_type = {
    'DEATH': 'DEATH',
    'RECOVERED': 'RECOVERED',
    'CONFIRMED': 'CONFIRMED'
};

function _insertCasePromise(type, coronaCase, self, insertedCases) {
    return self.insertCoronaCase(type, coronaCase).then((res) => {
        if (res) insertedCases.push(res);

        return res;
    });
}

function _updateCasePromise(type, coronaCase, self, updatedCases) {
    return new Promise((resolve, reject) => {
        self.hasCase(type, coronaCase.id).then((res) => {
            if (res == true) {
                switch (type) {
                    case corona_info_type.CONFIRMED:
                        self.updateConfirmedCase(coronaCase).then((res) => {
                          if (res && res.status) updatedCases.push(res);
                          resolve();
                        });
                        break;
                    case corona_info_type.DEATH:
                        self.updateDeadCase(coronaCase).then((res) => {
                            if (res && res.status) updatedCases.push(res);
                        });
                        resolve();
                        break;
                    case corona_info_type.RECOVERED:
                        self.updateRecoveredCase(coronaCase).then((res) => {
                            if (res && res.status) updatedCases.push(res);
                        });
                        resolve();
                        break;
                    default:
                        reject(`Unknown type ${type}`);
                        break;
                }
            } else {
                self.insertCoronaCase(type, coronaCase).then((res) => {
                    if (res) updatedCases.push(res);
                    resolve();
                });
            }
        }).catch((e) => {
            console.error(`Error getting case ${coronaCase.id} with type ${type}`);
            console.log(e);
            reject(`error getting case ${coronaCase.id} with type ${type}`);
        });
    });
}

module.exports = {
    insertCases(cases) {
        return new Promise((resolve, reject) => {
            var insertedCases = [];
            if (!cases) {
                if (DEBUG_MODE) {
                    console.log('Nothing to store!');
                }
                resolve({status: 0, message: 'nothing to insert'});
            } else {
                var promises = [];

                for (const coronaCase of cases.confirmed) {
                    promises.push(_insertCasePromise(corona_info_type.CONFIRMED, coronaCase, this, insertedCases));
                }
                for (const coronaCase of cases.deaths) {
                    promises.push(_insertCasePromise(corona_info_type.DEATH, coronaCase, this, insertedCases));
                }
                for (const coronaCase of cases.recovered) {
                    promises.push(_insertCasePromise(corona_info_type.RECOVERED, coronaCase, this, insertedCases));
                }

                Promise.all(promises).then(() => {
                    resolve({status: 1, message: `${insertedCases.length} cases inserted`});
                }).catch((e) => {
                    console.log('Error inserting cases');
                    console.log(e);
                    reject(e);
                });
            }
        });
    },
    updateCases(cases) {
        return new Promise((resolve, reject) => {
            var updatedCases = [];
            if (!cases) {
                if (DEBUG_MODE) {
                    console.log('Nothing to update!');
                }
                resolve({status: 0, message: 'nothing to update'});
            } else {
                var promises = [];
                for (const coronaCase of cases.confirmed) {
                    promises.push(_updateCasePromise(corona_info_type.CONFIRMED, coronaCase, this, updatedCases));
                }
                for (const coronaCase of cases.deaths) {
                    promises.push(_updateCasePromise(corona_info_type.DEATH, coronaCase, this, updatedCases));
                }
                for (const coronaCase of cases.recovered) {
                    promises.push(_updateCasePromise(corona_info_type.RECOVERED, coronaCase, this, updatedCases));
                }

                Promise.all(promises).then(() => {
                    resolve({status: 1, message: `${updatedCases.length} cases updated`});
                }).catch((e) => {
                    console.log('Error inserting cases');
                    console.log(e);
                    reject(e);
                });
            }
        });
    },
    insertCoronaCase(type, coronaCase) {
        return new Promise((resolve, reject) => {
            var tableName = '';
            console.log(`Inserting ${coronaCase.id} to ${type}`);
            switch (type) {
                case corona_info_type.CONFIRMED:
                    tableName = CONFIRMED_TABLE;
                    break;
                case corona_info_type.DEATH:
                    tableName = DEATHS_TABLE;
                    break;
                case corona_info_type.RECOVERED:
                    tableName = RECOVERED_TABLE;
                    break;
                default:
                    tableName = '';
                    break;
            }

            const params = {
                TableName: tableName,
                Item: coronaCase,
                ConditionExpression: 'attribute_not_exists(id)'
            };

            dynamoDb.put(params, function (err) {
                if (err && err.code !== 'ConditionalCheckFailedException') {
                    console.log(`Error inserting case ${coronaCase.id} to ${tableName}`);
                    console.log(err);
                    reject(err);
                } else if (err && err.code === 'ConditionalCheckFailedException') {
                    resolve();
                } else {
                    resolve(coronaCase);
                }
            });
        });
    },
    hasCase(type, id) {
        return new Promise((resolve, reject) => {
            var tableName = '';

            switch (type) {
                case corona_info_type.CONFIRMED:
                    tableName = CONFIRMED_TABLE;
                    break;
                case corona_info_type.DEATH:
                    tableName = DEATHS_TABLE;
                    break;
                case corona_info_type.RECOVERED:
                    tableName = RECOVERED_TABLE;
                    break;
                default:
                    tableName = '';
                    break;
            }

            var params = {
                TableName: tableName,
                Key: {
                    id: id
                },
                ProjectionExpression: '#id',
                ExpressionAttributeNames: {
                    '#id': 'id'
                }
            };
            dynamoDb.get(params, function(err, data) {
                if (err) {
                    console.log(`Error getting case ${id} by type ${type}`);
                    console.log(err);
                    reject(err);
                } else {
                    if (data.Item) {
                        resolve(true);
                    } else {
                        resolve(false);
                    }
                }
            });
        });
    },
    updateConfirmedCase(coronaCase) {
        return new Promise((resolve, reject) => {
            console.log(`Updating confirmed ${coronaCase.id}`);
            var params = {
                TableName: CONFIRMED_TABLE,
                Key: {
                    id: coronaCase.id
                },
                UpdateExpression: 'set #hcd = :hcd, #is = :is, #isc = :isc',
                ExpressionAttributeNames: {
                    '#hcd': 'healthCareDistrict',
                    '#is': 'infectionSource',
                    '#isc': 'infectionSourceCountry'
                },
                ExpressionAttributeValues: {
                    ':hcd': coronaCase.healthCareDistrict,
                    ':is': coronaCase.infectionSource,
                    ':isc': coronaCase.infectionSourceCountry
                },
                ReturnValues: 'UPDATED_OLD'
            };

            dynamoDb.update(params, function (err, data) {
                if (err) {
                    console.log('Error while updating confirmed');
                    console.log(err);
                    reject(err);
                } else {
                    var status = 0;
                    if (data.Attributes && (data.Attributes.healthCareDistrict != coronaCase.healthCareDistrict || data.Attributes.infectionSource != coronaCase.infectionSource || data.Attributes.infectionSourceCountry != coronaCase.infectionSourceCountry)) {
                        status = 1;
                    }
                    resolve({status: status, message: 'success'});
                }
            });
        });
    },
    updateRecoveredCase(coronaCase) {
        return new Promise((resolve, reject) => {
            console.log(`Updating recovered ${coronaCase.id}`);
            var params = {
                TableName: RECOVERED_TABLE,
                Key: {
                    id: coronaCase.id
                },
                UpdateExpression: 'set #hcd = :hcd',
                ExpressionAttributeNames: {
                    '#hcd': 'healthCareDistrict'
                },
                ExpressionAttributeValues: {
                    ':hcd': coronaCase.healthCareDistrict
                },
                ReturnValues: 'UPDATED_OLD'
            };

            dynamoDb.update(params, function (err, data) {
                if (err) {
                    console.log('Error while updating recovered');
                    console.log(err);
                    reject(err);
                } else {
                    var status = 0;
                    if (data.Attributes && data.Attributes.healthCareDistrict != coronaCase.healthCareDistrict ) {
                        status = 1;
                    }
                    resolve({status: status, message: 'success'});
                }
            });
        });
    },
    updateDeadCase(coronaCase) {
        return new Promise((resolve, reject) => {
            console.log(`Updating death ${coronaCase.id}`);
            resolve({status: 0, message: 'success'});
        });
    },
    updateOperation(operation) {
        return new Promise((resolve, reject) => {
            var d = new Date();
            var params = {
                TableName: OPERATIONS_TABLE,
                Key: {
                    name: operation.name
                },
                UpdateExpression: 'set #yr = :yr, #mon = :mon, #day = :day, #hour = :hour, #minute = :minute',
                ExpressionAttributeNames: {
                    '#yr': 'yr',
                    '#mon': 'mon',
                    '#day': 'day',
                    '#hour': 'hour',
                    '#minute': 'minute'
                },
                ExpressionAttributeValues: {
                    ':yr': d.getFullYear(),
                    ':mon': d.getMonth(),
                    ':day': d.getDate(),
                    ':hour': d.getHours(),
                    ':minute': d.getMinutes()
                }
            };

            dynamoDb.update(params, function (err, data) {
                if (err) {
                    console.log('Error while updating operation');
                    console.log(err);
                    reject(err);
                } else {
                    resolve({status: 1, message: 'success'});
                }
            });
        });
    },
    getOldestOperation(maintype) {
        return new Promise((resolve, reject) => {
            var params = {
                TableName: OPERATIONS_TABLE,
                FilterExpression: '#maintype = :maintype and #active = :istrue',
                ExpressionAttributeNames: {
                    '#maintype': 'maintype',
                    '#active': 'active'
                },
                ExpressionAttributeValues: {
                    ':maintype': maintype,
                    ':istrue': true
                }
            };

            utils.performScan(dynamoDb, params).then((operations) => {
                if (!operations || !operations.length) {
                    reject('No operations found');
                } else {
                    for (const op of operations) {
                        var d = new Date(op.yr, op.mon, op.day, op.hour, op.minute);
                        op.lastRun = d;
                    }
                    operations.sort(function (a, b) {
                        return a.lastRun - b.lastRun;
                    });

                    resolve(operations[0]);
                }
            }).catch((e) => {
                console.log('Error getting operations');
                console.log(e);
                reject(e);
            });
        });
    }
};
