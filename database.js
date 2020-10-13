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
const CONFIRMED_SORT_INDEX = process.env.CONFIRMED_SORT_INDEX || '';
const DEATHS_TABLE = process.env.DEATHS_TABLE;
const RECOVERED_TABLE = process.env.RECOVERED_TABLE;
const CHARTS_TABLE = process.env.CHARTS_TABLE;
const THL_CASES_LINK = process.env.THL_CASES_LINK;
const CASE_BUCKET = process.env.CASE_BUCKET;
const UPDATE_TRESHOLD_DAYS = process.env.UPDATE_TRESHOLD_DAYS || 8;
const HOSPITALIZED_TABLE = process.env.HOSPITALIZED_TABLE;
const DATE_TIME_SORT_STRING_FORMAT = 'YYYY-MM-DD HH:mm:ss';
const DATE_SORT_STRING_FORMAT = 'YYYY-MM-DD';
const SORT_STRING_COL_NAME = 'dateSortString';
const CORONA_INFO_TYPE = {
    'DEATH': 'DEATH',
    'RECOVERED': 'RECOVERED',
    'CONFIRMED': 'CONFIRMED',
    'HOSPITALIZATION': 'HOSPITALIZATION'
};
const OPERATION_TYPE = {
    'INSERT': 'INSERT',
    'UPDATE': 'UPDATE',
    'DELETE': 'DELETE'
};

function _getOperationType(list, id) {
    return _.chain(list)
        .filter(function (c) {
            return !c.isremoved;
        })
        .map(function (c) {
            return c.id;
        })
        .contains(id)
        .value() ? OPERATION_TYPE.UPDATE : OPERATION_TYPE.INSERT;
}

function _getDifference(oldCases, inputCases) {
    var oldMap = _.chain(oldCases).filter(function (c) { return !c.isremoved; }).map(function(c) {return c.id; }).value();
    var inputMap = _.chain(inputCases).map(function (c) { return c.id; }).value();
    var d = []; //_.difference(oldMap, inputMap);

    for (const old of oldMap) {
        if (inputMap.indexOf(old) == -1) d.push(old);
    }

    return d;
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
                for (const hospitalization of cases.hospitalizations) {
                    promises.push(this.insertHospitalization(hospitalization).then((res) => {
                        if (res) this.insertCases.push(res);
                    }));
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
    updateCases(cases, tresholdDays) {
        return new Promise((resolve, reject) => {
            var treshold = tresholdDays || UPDATE_TRESHOLD_DAYS;
            var updatedCases = [];
            var m = moment();
            if (!cases) {
                if (DEBUG_MODE) {
                    console.log('Nothing to update!');
                }
                resolve({status: 0, message: 'nothing to update'});
            } else {
                var initialPromises = [];
                var confirmed = [];
                var deaths = [];
                var recovered = [];
                var hospitalizations = [];
                var inputCasesTreshold = moment().subtract(treshold, 'days');
                const inputCasesTresholdString = inputCasesTreshold.format(DATE_SORT_STRING_FORMAT);

                for (const cc of cases.confirmed) {
                    if (moment(cc.date).isAfter(inputCasesTreshold)) {
                        confirmed.push(cc);
                    }
                }
                for (const cc of cases.deaths) {
                    if (moment(cc.date).isAfter(inputCasesTreshold)) {
                        deaths.push(cc);
                    }
                }
                for (const cc of cases.recovered) {
                    if (moment(cc.date).isAfter(inputCasesTreshold)) {
                        recovered.push(cc);
                    }
                }
                for (const hh of cases.hospitalizations) {
                    if (moment(hh.date).format(DATE_SORT_STRING_FORMAT) >= inputCasesTresholdString) {
                        hospitalizations.push(hh);
                    }
                }

                initialPromises.push(this.getCaseInfos(CORONA_INFO_TYPE.CONFIRMED, treshold));
                initialPromises.push(this.getCaseInfos(CORONA_INFO_TYPE.DEATH, treshold));
                initialPromises.push(this.getCaseInfos(CORONA_INFO_TYPE.RECOVERED, treshold));
                initialPromises.push(this.getHospitalizations(treshold));

                /*initialPromises.push(this.updateDateOfCases(CONFIRMED_TABLE));
                initialPromises.push(this.updateDateOfCases(DEATHS_TABLE));
                initialPromises.push(this.updateDateOfCases(RECOVERED_TABLE));*/
                /*initialPromises.push(this.resetRemovedFromTable(CONFIRMED_TABLE));
                initialPromises.push(this.resetRemovedFromTable(DEATHS_TABLE));
                initialPromises.push(this.resetRemovedFromTable(RECOVERED_TABLE));
                initialPromises.push(this.setInfectionSourceCountryToNullForAll(CONFIRMED_TABLE));
                initialPromises.push(this.addDateSortStrings(CONFIRMED_TABLE, '', 'country'));*/

                Promise.all(initialPromises).then((allInitialResults) => {
                    console.log('Initial promises got in ' + moment().diff(m) + ' milliseconds');
                    var promises = [];
                    var tresholdFilteredConfirmed = allInitialResults[0];
                    var tresholdFilteredDeaths = allInitialResults[1];
                    var tresholdFilteredRecovered = allInitialResults[2];
                    var tresholdFilteredHospitalizations = allInitialResults[3];
                    var initialResultHashTable = {};
                    var deathsHashTable = {};
                    var hospitalizationHashTable = {};

                    for (const filteredConfirmedCase of tresholdFilteredConfirmed) {
                        initialResultHashTable[filteredConfirmedCase.id] = filteredConfirmedCase;
                        if (filteredConfirmedCase.id == 'Kymenlaakso_2020-05-01T12:00:00.000Z_1' ) {
                            console.log('kurkkaa se');
                        }
                    }
                    for (const filteredDeath of tresholdFilteredDeaths) {
                        deathsHashTable[filteredDeath.id] = filteredDeath;
                    }
                    for (const filteredHospn of tresholdFilteredHospitalizations) {
                        var key = '' + filteredHospn.area + moment(filteredHospn.date).format(DATE_SORT_STRING_FORMAT);

                        hospitalizationHashTable[key] = filteredHospn;
                    }

                    for (const toDelete of _getDifference(tresholdFilteredConfirmed, confirmed)) {
                        promises.push(this.markAsDeleted(CORONA_INFO_TYPE.CONFIRMED, toDelete));
                        if (toDelete == 'Kymenlaakso_2020-05-01T12:00:00.000Z_1') {
                            console.log('merkataan poistetuksi');
                        }
                        updatedCases.push(toDelete);
                    }
                    for (const toDelete of _getDifference(tresholdFilteredDeaths, deaths)) {
                        promises.push(this.markAsDeleted(CORONA_INFO_TYPE.DEATH, toDelete));
                        updatedCases.push(toDelete);
                    }
                    for (const toDelete of _getDifference(tresholdFilteredRecovered, recovered)) {
                        promises.push(this.markAsDeleted(CORONA_INFO_TYPE.RECOVERED, toDelete));
                        updatedCases.push(toDelete);
                    }
                    console.log('Old deletions handled in ' + moment().diff(m) + ' milliseconds');
                    for (const coronaCase of confirmed) {
                        var shouldUpdateOrInsert = true;
                        const oldCase = initialResultHashTable[coronaCase.id];
                        if (coronaCase.id == 'Kymenlaakso_2020-05-01T12:00:00.000Z_1' ) {
                            console.log('yritetään käsitellä');
                        }
                        if (oldCase) {
                            if (oldCase.id == 'Kymenlaakso_2020-05-01T12:00:00.000Z_1' ) {
                                console.log('löyty vanha');
                            }
                            if (oldCase.healthCareDistrict != coronaCase.healthCareDistrict || oldCase.infectionSource != coronaCase.infectionSource || oldCase.infectionSourceCountry != coronaCase.infectionSourceCountry) {
                                shouldUpdateOrInsert = true;
                            } else {
                                shouldUpdateOrInsert = false;
                            }
                        }

                        if (shouldUpdateOrInsert) {
                            promises.push(_updateCasePromise(_getOperationType(tresholdFilteredConfirmed, coronaCase.id), CORONA_INFO_TYPE.CONFIRMED, coronaCase, this, updatedCases));
                        }
                    }
                    for (const coronaCase of deaths) {
                        var shouldUpdateOrInsertDeath = true;
                        const oldDeath = deathsHashTable[coronaCase.id];

                        if (oldDeath) {
                            if (oldDeath.healthCareDistrict != coronaCase.healthCareDistrict) {
                                shouldUpdateOrInsertDeath = true;
                            } else {
                                shouldUpdateOrInsertDeath = false;
                            }
                        }

                        if (shouldUpdateOrInsertDeath) {
                            promises.push(_updateCasePromise(_getOperationType(tresholdFilteredDeaths, coronaCase.id), CORONA_INFO_TYPE.DEATH, coronaCase, this, updatedCases));
                        }
                    }
                    for (const coronaCase of recovered) {
                        promises.push(_updateCasePromise(_getOperationType(tresholdFilteredRecovered, coronaCase.id), CORONA_INFO_TYPE.RECOVERED, coronaCase, this, updatedCases));
                    }
                    console.log('Update promises handled in ' + moment().diff(m) + ' milliseconds');
                    for (const hospitalization of hospitalizations) {
                        var shouldUpdateOrInsertHospitalization = true;
                        var isNew = true;
                        var kkey = '' + hospitalization.area + moment(hospitalization.date).format(DATE_SORT_STRING_FORMAT);
                        var oldHospitalization = hospitalizationHashTable[kkey];

                        if (oldHospitalization) {
                            isNew = false;
                            if (hospitalization.dead != oldHospitalization.dead || hospitalization.inIcu != oldHospitalization.inIcu || hospitalization.inWard != oldHospitalization.inWard || hospitalization.totalHospitalised != oldHospitalization.totalHospitalised) {
                                shouldUpdateOrInsertHospitalization = true;
                            } else {
                                shouldUpdateOrInsertHospitalization = false;
                            }

                            if (oldHospitalization.longDateString < moment(hospitalization.date).format(DATE_TIME_SORT_STRING_FORMAT)) {
                                shouldUpdateOrInsertHospitalization = false;
                            }
                        }

                        if (shouldUpdateOrInsertHospitalization) {
                            if (isNew) {
                                promises.push(this.insertHospitalization(hospitalization).then((res) => { //jshint ignore:line
                                    if (res) updatedCases.push(res);
                                }));
                            } else {
                                promises.push(this.updateHospitalizationWithPromise(hospitalization).then((res) => { //jshint ignore:line
                                    if (res) updatedCases.push(res);
                                }));
                            }
                        }
                    }
                    console.log('Hospitalizations handled in ' + moment().diff(m) + ' milliseconds');
                    return Promise.all(promises);
                }).then(() => {
                    console.log('All handled in ' + moment().diff(m) + ' milliseconds');
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

            coronaCase[SORT_STRING_COL_NAME] = moment(coronaCase.date).format(DATE_TIME_SORT_STRING_FORMAT);
            coronaCase.insertDateSortString = moment(coronaCase.insertDate).format(DATE_TIME_SORT_STRING_FORMAT);

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
    insertHospitalization(hospitalization) {
        return new Promise((resolve, reject) => {
            const self = this;
            const dateFormatted = moment(hospitalization.date).format(DATE_SORT_STRING_FORMAT);
            hospitalization[SORT_STRING_COL_NAME] = dateFormatted;
            hospitalization.longDateString = moment(hospitalization.date).format(DATE_TIME_SORT_STRING_FORMAT);
            hospitalization.date = dateFormatted;
            var params = {
                TableName: HOSPITALIZED_TABLE,
                Item: hospitalization,
                ConditionExpression: 'attribute_not_exists(area) AND attribute_not_exists(#date)',
                ExpressionAttributeNames: {
                    '#date': 'date'
                }
            };

            dynamoDb.put(params, function (err) {
                if (err&& err.code !== 'ConditionalCheckFailedException') {
                    console.log('Error inserting hospitalization');
                    console.log(err);
                    reject(err);
                } else if (err && err.code === 'ConditionalCheckFailedException') {
                    self.updateHospitalization(hospitalization, resolve, reject);
                } else {
                    resolve(hospitalization);
                }
            });
        });
    },
    updateHospitalizationWithPromise(hospitalization) {
        return new Promise((resolve, reject) => {
            this.updateHospitalization(hospitalization, resolve, reject);
        });
    },
    updateHospitalization(hospitalization, resolve, reject) {
        var params = {
            TableName: HOSPITALIZED_TABLE,
            Key: {
                'area': hospitalization.area,
                'date': hospitalization.date
            },
            UpdateExpression: 'set #dead = :dead, #inIcu = :inIcu, #inWard = :inWard, #totalHospitalised = :totalHospitalised',
            ExpressionAttributeNames: {
                '#dead': 'dead',
                '#inIcu': 'inIcu',
                '#inWard': 'inWard',
                '#totalHospitalised': 'totalHospitalised'
            },
            ExpressionAttributeValues: {
                ':dead': hospitalization.dead,
                ':inIcu': hospitalization.inIcu,
                ':inWard': hospitalization.inWard,
                ':totalHospitalised': hospitalization.totalHospitalised
            }
        };

        dynamoDb.update(params, function (err, data) {
            if (err) {
                console.log('Error while updating confirmed');
                console.log(err);
                reject(err);
            } else {
                resolve(hospitalization);
            }
        });
    },
    getHospitalizations(fromSinceDays = 0) {
        return new Promise((resolve, reject) => {
            var params = {
                TableName: HOSPITALIZED_TABLE,
                FilterExpression: '#sortString >= :sortStringTreshold',
                ExpressionAttributeNames: {
                    '#sortString': SORT_STRING_COL_NAME
                },
                ExpressionAttributeValues: {
                    ':sortStringTreshold': moment().subtract(fromSinceDays, 'days').format(DATE_SORT_STRING_FORMAT)
                }
            };

            utils.performScan(dynamoDb, params).then((results) => {
                if (!results || !results.length) {
                    resolve([]);
                } else {
                    resolve(results);
                }
            }).catch((e) => {
                console.log('Error getting hospitalizations');
                console.log(e);
                reject(e);
            });
        });
    },
    getCaseInfos(type, fromSinceDays) {
        return new Promise((resolve, reject) => {
            var tableName = '';
            var indexName = '';
            var queryType = 'scan';
            const dateStringTreshold = moment().subtract(fromSinceDays, 'days').format(DATE_TIME_SORT_STRING_FORMAT);
            console.log('date string treshold is');
            console.log(dateStringTreshold);

            switch (type) {
                case CORONA_INFO_TYPE.CONFIRMED:
                    tableName = CONFIRMED_TABLE;
                    indexName = CONFIRMED_SORT_INDEX;
                    queryType = 'query';
                    break;
                case CORONA_INFO_TYPE.DEATH:
                    tableName = DEATHS_TABLE;
                    break;
                case CORONA_INFO_TYPE.RECOVERED:
                    tableName = RECOVERED_TABLE;
                    break;
                default:
                    console.log(`type was ${type}`);
                    tableName = '';
                    break;
            }

            var params = {
                TableName: tableName,
                ProjectionExpression: '#id, #isremoved, #healthCareDistrict, #infectionSource, #infectionSourceCountry, #date',
                FilterExpression: '#sortString > :sortStringTreshold and #country = :country',
                ExpressionAttributeNames: {
                    '#id': 'id',
                    '#isremoved': 'isremoved',
                    '#healthCareDistrict': 'healthCareDistrict',
                    '#infectionSource': 'infectionSource',
                    '#infectionSourceCountry': 'infectionSourceCountry',
                    '#country': 'country',
                    '#date': 'date',
                    '#sortString': SORT_STRING_COL_NAME
                },
                ExpressionAttributeValues: {
                    ':sortStringTreshold' : dateStringTreshold,
                    ':country': 'FIN'
                }
            };

            if (indexName) {
                params.IndexName = indexName;
                params.KeyConditionExpression = params.FilterExpression;
                delete params.FilterExpression;
            }

            switch (queryType) {
                case 'query':
                    dynamoDb.query(params, function (err, data) {
                        if (err) {
                            console.log(`Error loading ${type}`);
                            console.log(err);
                            reject(err);
                        } else {
                            resolve(data.Items);
                        }
                    });
                    break;
                case 'scan':
                default:
                    utils.performScan(dynamoDb, params).then((cases) => {
                        if (!cases || !cases.length) {
                            resolve([]);
                        } else {
                            resolve(cases);
                        }
                    }).catch((e) => {
                        console.error('error getting cases from db');
                        console.log(e);
                        reject(e);
                    });
                    break;
            }
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
    /**
     * Updates each item with result string type array from date
     *
     * @param {string} tableName to update
     * @param {string} sourceCol to get the value from
     * @param {string} targetCol where to insert value
     */
    addDateSortStrings(tableName, sourceCol, targetCol) {
        return new Promise((resolve, reject) => {
            var scanParams = {
                TableName: tableName
            };

            utils.performScan(dynamoDb, scanParams).then((items) => {
                var promises = [];
                for (const item of items) {
                    var d = moment(item[sourceCol]).format(DATE_TIME_SORT_STRING_FORMAT);
                    promises.push(this.updateDateSortStringToItem(tableName, targetCol, d, {id: item.id}));
                }
                return Promise.all(promises);
            }).then(() => {
                resolve();
            }).catch((e) => {
                console.log('Error getting confirmed cases');
                console.log(e);
                reject(e);
            });
        });
    },
    updateDateOfCases(tableName) {
        return new Promise((resolve, reject) => {
            var params = {
                TableName: tableName
            };

            utils.performScan(dynamoDb, params).then((items) => {
                var promises = [];
                for (const item of items) {
                    promises.push(this.updateDateOfItem(tableName, {id: item.id}, item.date));
                }
                return Promise.all(promises);
            }).then(() => {
                resolve();
            }).catch((e) => {
                console.log('Error getting cases');
                console.log(e);
                reject(e);
            });
        });
    },
    updateDateOfItem(tableName, key, oldValue) {
        return new Promise((resolve, reject) => {
            var newDateString = moment(oldValue, utils.getDefaultInboundDateTimeFormat()).format(utils.getDateTimeFormat());
            var params = {
                TableName: tableName,
                Key: key,
                UpdateExpression: 'set #d = :dv',
                ExpressionAttributeNames: {
                    '#d': 'date'
                },
                ExpressionAttributeValues: {
                    ':dv': newDateString
                }
            };

            dynamoDb.update(params, function (err) {
                if (err) {
                    console.log('Error updating date');
                    console.log(err);
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    },
    setInfectionSourceCountryToNullForAll(tableName) {
        return new Promise((resolve, reject) => {
            var params = {
                TableName: tableName
            };

            utils.performScan(dynamoDb, params).then((items) => {
                var promises = [];
                for (const item of items) {
                    promises.push(this.setInfectionSourceCountryToNull(tableName, {id: item.id}));
                }
                return Promise.all(promises);
            }).then(() => {
                resolve();
            }).catch((e) => {
                console.log('Error getting cases');
                console.log(e);
                reject(e);
            });
        });
    },
    setInfectionSourceCountryToNull(tableName, key) {
        return new Promise((resolve, reject) => {
            var params = {
                TableName: tableName,
                Key: key,
                UpdateExpression: 'set #isc = :nll',
                ExpressionAttributeNames: {
                    '#isc': 'infectionSourceCountry'
                },
                ExpressionAttributeValues: {
                    ':nll': null
                }
            };

            dynamoDb.update(params, function (err) {
                if (err) {
                    console.log('Error setting isc to null');
                    console.log(err);
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    },
    resetRemovedFromTable(tableName) {
        return new Promise((resolve, reject) => {
            var scanParams = {
                TableName: tableName
            };

            utils.performScan(dynamoDb, scanParams).then((items) => {
                var promises = [];
                for (const item of items) {

                    promises.push(this.resetRemovedFromItem(tableName, {id: item.id}));
                }
                return Promise.all(promises);
            }).then(() => {
                resolve();
            }).catch((e) => {
                console.log('Error getting some cases');
                console.log(e);
                reject(e);
            });
        });
    },
    resetRemovedFromItem(tableName, key) {
        return new Promise((resolve, reject) => {
            var params = {
                TableName: tableName,
                Key: key,
                UpdateExpression: 'set #isremoved = :isremoved',
                ExpressionAttributeNames: {
                    '#isremoved': 'isremoved'
                },
                ExpressionAttributeValues: {
                    ':isremoved': false
                }
            };

            dynamoDb.update(params, function (err, data) {
                if (err) {
                    console.log('Error resetting removed');
                    console.log(err);
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    },
    updateDateSortStringToItem(tableName, targetCol, value, key) {
        return new Promise((resolve, reject) => {
            var params = {
                TableName: tableName,
                Key: key,
                UpdateExpression: 'set #target = :val',
                ExpressionAttributeNames: {
                    '#target': targetCol
                },
                ExpressionAttributeValues: {
                    ':val': value
                }
            };

            dynamoDb.update(params, function (err, data) {
                if (err) {
                    console.log('Error updating date sort string');
                    console.log(err);
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }
};
