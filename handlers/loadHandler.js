/*jslint node: true */
/*jshint esversion: 6 */
'use strict';

const openHSLoader = require('../loaders/openHSLoader');
const thlLoader = require('../loaders/thlLoader');
const storeHandler = require('./storeHandler');
const database = require('../database');
const chartHandler = require('./chartHandler');
const DEBUG_MODE = process.env.DEBUG_MODE === 'ON';
const HCD_CHARTS_RECRETE_TRESHOLD = process.env.HCD_CHARTS_RECRETE_TRESHOLD ? parseInt(process.env.HCD_CHARTS_RECRETE_TRESHOLD, 10) : 0;

function _load(operation) {
    return new Promise((resolve, reject) => {
        switch (operation.type) {
            case 'HSOpen':
                openHSLoader.load(operation, resolve, reject);
                break;
            case 'THL':
                thlLoader.load(operation, resolve, reject);
                break;
            default:
                reject({status: 0, message: 'No matching source given'});
                break;
        }
    });
}

function _storeResults(results, isUpdate, tresholdDays) {
    switch (results.type) {
        case 'HSOpen':
            console.log('storing');
            return storeHandler.storeHSOpen(results.cases, isUpdate, tresholdDays);
        case 'THL':
            return storeHandler.storeTHL(results.cases);
        default:
            return new Promise((resolve, reject) => {
                reject({status: 0, message: 'No matching store procedure given'});
            });
    }
}

module.exports = {
    autoLoad(isUpdate, tresholdDays) {
        return new Promise((resolve, reject) => {
            database.getOldestOperation('coronaloader').then((operation) => {
                _load(operation).then((results) => {
                    _storeResults(results, isUpdate, tresholdDays).then((insertResult) => {
                        console.log('Insertions done');
                        if (DEBUG_MODE) {
                            console.log('Insertion result:');
                            console.log(insertResult);
                        }
                        var finalPromises = [];

                        if (insertResult.amount) {
                            if (insertResult.amount > HCD_CHARTS_RECRETE_TRESHOLD) {
                                finalPromises.push(chartHandler.createAllCharts());
                            } else {
                                finalPromises.push(chartHandler.createCharts());
                            }

                            //finalPromises.push(chartHandler.createHospitalizationCharts());
                        } else {
                            finalPromises.push(new Promise((innerResolve, innerReject) => {
                                innerResolve();
                            }));
                        }

                        Promise.all(finalPromises).then(() => {
                            console.log('Final promises done');
                            resolve(`Data loaded by operation ${operation.name} from ${operation.type}. ${results.message}`);
                        }).catch((e) => {
                            console.log('Failed to update charts');
                            console.log(e);
                            reject(e);
                        });
                    }).catch((e) => {
                        reject(e);
                    });
                }).catch((e) => {
                    reject(e);
                });
            }).catch((e) => {
                reject(e);
            });
        });
    }
};