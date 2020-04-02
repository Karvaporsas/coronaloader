/*jslint node: true */
/*jshint esversion: 6 */
'use strict';

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const _ = require('underscore');
const moment = require('moment');
const fs = require('fs');
const utils = require('./utils');
const OPERATIONS_TABLE = process.env.TABLE_OPERATIONS;
const DEBUG_MODE = process.env.DEBUG_MODE === 'ON';
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const CONFIRMED_TABLE = process.env.CONFIRMED_TABLE;
const DEATHS_TABLE = process.env.DEATHS_TABLE;
const RECOVERED_TABLE = process.env.RECOVERED_TABLE;
const CHARTS_TABLE = process.env.CHARTS_TABLE;
const THL_CASES_LINK = process.env.THL_CASES_LINK;
const CASE_BUCKET = process.env.CASE_BUCKET;
const CORONA_INFO_TYPE = {
    'DEATH': 'DEATH',
    'RECOVERED': 'RECOVERED',
    'CONFIRMED': 'CONFIRMED'
};
const OPERATION_TYPE = {
    'INSERT': 'INSERT',
    'UPDATE': 'UPDATE',
    'DELETE': 'DELETE'
};

function _getOperationType(list, id) {
    return _.chain(list)
        .map(function (c) {
            return c.id;
        })
        .contains(id)
        .value() ? OPERATION_TYPE.UPDATE : OPERATION_TYPE.INSERT;
}

function _getDifference(oldCases, inputCases) {
    var oldMap = _.map(oldCases, function(c) {return c.id; });
    var inputMap = _.map(inputCases, function (c) { return c.id; });

    return _.difference(oldMap, inputMap);
}

function _insertCasePromise(type, coronaCase, self, insertedCases) {
    return self.insertCoronaCase(type, coronaCase).then((res) => {
        if (res) insertedCases.push(res);

        return res;
    });
}

function _updateCasePromise(operationType, type, coronaCase, self, updatedCases) {
    return new Promise((resolve, reject) => {
        if (operationType == OPERATION_TYPE.UPDATE) {
            switch (type) {
                case CORONA_INFO_TYPE.CONFIRMED:
                    self.updateConfirmedCase(coronaCase).then((res) => {
                        if (res && res.status) updatedCases.push(res);
                        resolve();
                    });
                    break;
                case CORONA_INFO_TYPE.DEATH:
                    self.updateDeadCase(coronaCase).then((res) => {
                        if (res && res.status) updatedCases.push(res);
                    });
                    resolve();
                    break;
                case CORONA_INFO_TYPE.RECOVERED:
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
                    promises.push(_insertCasePromise(CORONA_INFO_TYPE.CONFIRMED, coronaCase, this, insertedCases));
                }
                for (const coronaCase of cases.deaths) {
                    promises.push(_insertCasePromise(CORONA_INFO_TYPE.DEATH, coronaCase, this, insertedCases));
                }
                for (const coronaCase of cases.recovered) {
                    promises.push(_insertCasePromise(CORONA_INFO_TYPE.RECOVERED, coronaCase, this, insertedCases));
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
                var initialPromises = [];

                initialPromises.push(this.getCaseInfos(CORONA_INFO_TYPE.CONFIRMED));
                initialPromises.push(this.getCaseInfos(CORONA_INFO_TYPE.DEATH));
                initialPromises.push(this.getCaseInfos(CORONA_INFO_TYPE.RECOVERED));

                Promise.all(initialPromises).then((allInitialResults) => {
                    var promises = [];
                    for (const toDelete of _getDifference(allInitialResults[0], cases.confirmed)) {
                        promises.push(this.markAsDeleted(CORONA_INFO_TYPE.CONFIRMED, toDelete));
                    }
                    for (const toDelete of _getDifference(allInitialResults[1], cases.deaths)) {
                        promises.push(this.markAsDeleted(CORONA_INFO_TYPE.DEATH, toDelete));
                    }
                    for (const toDelete of _getDifference(allInitialResults[2], cases.recovered)) {
                        promises.push(this.markAsDeleted(CORONA_INFO_TYPE.RECOVERED, toDelete));
                    }
                    for (const coronaCase of cases.confirmed) {
                        promises.push(_updateCasePromise(_getOperationType(allInitialResults[0], coronaCase.id), CORONA_INFO_TYPE.CONFIRMED, coronaCase, this, updatedCases));
                    }
                    for (const coronaCase of cases.deaths) {
                        promises.push(_updateCasePromise(_getOperationType(allInitialResults[1], coronaCase.id), CORONA_INFO_TYPE.DEATH, coronaCase, this, updatedCases));
                    }
                    for (const coronaCase of cases.recovered) {
                        promises.push(_updateCasePromise(_getOperationType(allInitialResults[2], coronaCase.id), CORONA_INFO_TYPE.RECOVERED, coronaCase, this, updatedCases));
                    }

                    return Promise.all(promises);
                }).then(() => {
                    resolve({status: 1, message: `${updatedCases.length} cases updated`, amount: updatedCases.length});
                }).catch((e) => {
                    console.log('error getting initial results on updateCases');
                    console.log(e);
                    reject(e);
                });
            }
        });
    },
    insertCoronaCase(type, coronaCase) {
        return new Promise((resolve, reject) => {
            var tableName = '';
            switch (type) {
                case CORONA_INFO_TYPE.CONFIRMED:
                    tableName = CONFIRMED_TABLE;
                    break;
                case CORONA_INFO_TYPE.DEATH:
                    tableName = DEATHS_TABLE;
                    break;
                case CORONA_INFO_TYPE.RECOVERED:
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
    getCaseInfos(type) {
        return new Promise((resolve, reject) => {
            var tableName = '';

            switch (type) {
                case CORONA_INFO_TYPE.CONFIRMED:
                    tableName = CONFIRMED_TABLE;
                    break;
                case CORONA_INFO_TYPE.DEATH:
                    tableName = DEATHS_TABLE;
                    break;
                case CORONA_INFO_TYPE.RECOVERED:
                    tableName = RECOVERED_TABLE;
                    break;
                default:
                    tableName = '';
                    break;
            }

            var params = {
                TableName: tableName,
                ProjectionExpression: '#id',
                ExpressionAttributeNames: {
                    '#id': 'id'
                }
            };

            utils.performScan(dynamoDb, params).then((cases) => {
                if (!cases || !cases.length) {
                    resolve([]);
                } else {
                    resolve(cases);
                }
            }).catch((e) => {
                console.error('error getting cases');
                console.log(e);
                reject(e);
            });
        });
    },
    markAsDeleted(type, id) {
        return new Promise((resolve, reject) => {
            var tableName = '';

            switch (type) {
                case CORONA_INFO_TYPE.CONFIRMED:
                    tableName = CONFIRMED_TABLE;
                    break;
                case CORONA_INFO_TYPE.DEATH:
                    tableName = DEATHS_TABLE;
                    break;
                case CORONA_INFO_TYPE.RECOVERED:
                    tableName = RECOVERED_TABLE;
                    break;
                default:
                    tableName = '';
                    break;
            }

            var params = {
                TableName: tableName,
                Key: {
                    'id': id
                },
                UpdateExpression: 'set #isremoved = :isremoved',
                ExpressionAttributeNames: {
                    '#isremoved': 'isremoved'
                },
                ExpressionAttributeValues: {
                    ':isremoved': true
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
                    if (data.Attributes && data.Attributes.isremoved == false) {
                        status = 1;
                    }
                    resolve({status: status, message: 'success'});
                }
            });
        });
    },
    updateConfirmedCase(coronaCase) {
        return new Promise((resolve, reject) => {
            var params = {
                TableName: CONFIRMED_TABLE,
                Key: {
                    id: coronaCase.id
                },
                UpdateExpression: 'set #hcd = :hcd, #is = :is, #isc = :isc, #isremoved = :isremoved',
                ExpressionAttributeNames: {
                    '#hcd': 'healthCareDistrict',
                    '#is': 'infectionSource',
                    '#isc': 'infectionSourceCountry',
                    '#isremoved': 'isremoved'
                },
                ExpressionAttributeValues: {
                    ':hcd': coronaCase.healthCareDistrict,
                    ':is': coronaCase.infectionSource,
                    ':isc': coronaCase.infectionSourceCountry,
                    ':isremoved': false
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
                    if (data.Attributes && (data.Attributes.healthCareDistrict != coronaCase.healthCareDistrict || data.Attributes.infectionSource != coronaCase.infectionSource || data.Attributes.infectionSourceCountry != coronaCase.infectionSourceCountry || data.Attributes.isremoved != false)) {
                        status = 1;
                    }
                    resolve({status: status, message: 'success'});
                }
            });
        });
    },
    updateRecoveredCase(coronaCase) {
        return new Promise((resolve, reject) => {
            var params = {
                TableName: RECOVERED_TABLE,
                Key: {
                    id: coronaCase.id
                },
                UpdateExpression: 'set #hcd = :hcd, #isremoved = :isremoved',
                ExpressionAttributeNames: {
                    '#hcd': 'healthCareDistrict',
                    '#isremoved': 'isremoved'
                },
                ExpressionAttributeValues: {
                    ':hcd': coronaCase.healthCareDistrict,
                    ':isremoved': false
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
                    if (data.Attributes && (data.Attributes.healthCareDistrict != coronaCase.healthCareDistrict || data.Attributes.isremoved != false)) {
                        status = 1;
                    }
                    resolve({status: status, message: 'success'});
                }
            });
        });
    },
    updateDeadCase(coronaCase) {
        return new Promise((resolve, reject) => {
            var params = {
                TableName: DEATHS_TABLE,
                Key: {
                    id: coronaCase.id
                },
                UpdateExpression: 'set #hcd = :hcd, #isremoved = :isremoved',
                ExpressionAttributeNames: {
                    '#hcd': 'healthCareDistrict',
                    '#isremoved': 'isremoved'
                },
                ExpressionAttributeValues: {
                    ':hcd': coronaCase.healthCareDistrict,
                    ':isremoved': false
                },
                ReturnValues: 'UPDATED_OLD'
            };

            dynamoDb.update(params, function (err, data) {
                if (err) {
                    console.log('Error while updating deaths');
                    console.log(err);
                    reject(err);
                } else {
                    var status = 0;
                    if (data.Attributes && (data.Attributes.healthCareDistrict != coronaCase.healthCareDistrict || data.Attributes.isremoved != false)) {
                        status = 1;
                    }
                    resolve({status: status, message: 'success'});
                }
            });
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
    },
    getConfirmedCases(dataSource) {
        switch (dataSource) {
            case 'S3':
                return this.getConfirmedCasesFromS3();
            case 'DB':
            default:
                return this.getConfirmedCasesFromDB();
        }

    },
    getConfirmedCasesFromS3() {
        return new Promise((resolve, reject) => {
            this.getChartLink(dynamoDb, THL_CASES_LINK).then((chartLink) => {
                const inputFilename = '/tmp/' + chartLink.url;
                const writeStream = fs.createWriteStream(inputFilename);
                const hcdNames = utils.getHCDNames();
                s3.getObject({
                    Bucket: CASE_BUCKET,
                    Key: chartLink.url
                }).createReadStream().pipe(writeStream);
                writeStream.on('finish', function() {
                    fs.readFile(inputFilename, 'utf8', function(err, data) {
                        if (err) {
                            console.log('Error reading case file');
                            console.log(err);
                            reject(err);
                        } else {
                            var parsedData = JSON.parse(data);
                            var results = [];
                            for (const hcd of hcdNames) {
                                var casesFromDistrict = parsedData[hcd];
                                for (const dayOfHCD of casesFromDistrict) {
                                    for (let index = 0; index < dayOfHCD.value; index++) {
                                        var md = moment(dayOfHCD.date);
                                        var mdZoneAdjusted = moment(dayOfHCD.date).subtract(3, 'hours');
                                        results.push({
                                            day: md.format('YYYY-MM-DD'),
                                            date: mdZoneAdjusted,
                                            healthCareDistrict: dayOfHCD.healthCareDistrict,
                                            insertDate: md
                                        });
                                    }
                                }
                            }

                            resolve(results);
                        }

                    });
                });
                writeStream.on('error', function (err) {
                    console.log('Error getting image from S3');
                    console.log(err);
                    reject(err);
                });
            }).catch((e) => {
                console.log('Error getting chart link');
                console.log(e);
                reject(e);
            });
        });
    },
    getConfirmedCasesFromDB() {
        return new Promise((resolve, reject) => {
            var params = {
                TableName: CONFIRMED_TABLE,
                FilterExpression: '#isremoved <> :isremoved',
                ExpressionAttributeNames: {
                    '#isremoved': 'isremoved'
                },
                ExpressionAttributeValues: {
                    ':isremoved': true
                }
            };

            utils.performScan(dynamoDb, params).then((confirmedCases) => {
                for (const cc of confirmedCases) {
                    var d = moment(cc.date);
                    cc.day = d.format('YYYY-MM-DD');
                    cc.date = d;
                }
                resolve(confirmedCases);
            }).catch((e) => {
                console.log('Error getting confirmed cases');
                console.log(e);
                reject(e);
            });
        });
    },
    updateChartLink(link) {
        return new Promise((resolve, reject) => {
            var dateString = moment().format('YYYY-MM-DD HH:mm:ss');
            var params = {
                TableName: CHARTS_TABLE,
                Key: {
                    chartName: link.chartName
                },
                UpdateExpression: 'set #updated = :updated, #url = :url',
                ExpressionAttributeNames: {
                    '#updated': 'updated',
                    '#url': 'url'
                },
                ExpressionAttributeValues: {
                    ':updated': dateString,
                    ':url': link.url
                }
            };

            dynamoDb.update(params, function(err, data) {
                if (err) {
                    console.log('Error updating chartlink');
                    console.log(err);
                    reject(err);
                } else {
                    resolve({status: 1, message: 'success', link: link});
                }
            });
        });
    },
    getChartLink(dynamoDb, chartName){
        return new Promise((resolve, reject) => {
            var params = {
                TableName: CHARTS_TABLE,
                Key: {
                    chartName: chartName
                },
                ProjectionExpression: '#chartName, #updated, #url, #used',
                ExpressionAttributeNames: {
                    '#chartName': 'chartName',
                    '#updated': 'updated',
                    '#url': 'url',
                    '#used': 'used'
                }
            };
            dynamoDb.get(params, function (err, data) {
                if (err || !data) {
                    console.log('Error getting link');
                    console.log(err);
                    reject(err);
                } else {
                    resolve(data.Item);
                }
            });
        });
    },
};
