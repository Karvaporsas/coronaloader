/*jslint node: true */
/*jshint esversion: 6 */
'use strict';

const openHSLoader = require('../loaders/openHSLoader');
const storeHandler = require('./storeHandler');
const database = require('../database');
const chartHandler = require('./chartHandler');
const DEBUG_MODE = process.env.DEBUG_MODE === 'ON';

function _load(operation) {
    return new Promise((resolve, reject) => {
        switch (operation.type) {
            case 'HSOpen':
                openHSLoader.load(operation, resolve, reject);
                break;
            default:
                reject({status: 0, message: 'No matching source given'});
                break;
        }
    });
}

module.exports = {
    autoLoad(isUpdate) {
        return new Promise((resolve, reject) => {
            database.getOldestOperation('coronaloader').then((operation) => {
                _load(operation).then((results) => {
                    console.log('Loading was successful');
                    storeHandler.store(results.cases, isUpdate).then((insertResult) => {
                        if (DEBUG_MODE) {
                            console.log('Insertion result:');
                            console.log(insertResult);
                        }
                        var finalPromises = [];

                        if (insertResult.amount) {
                            finalPromises.push(chartHandler.createCharts());
                        } else {
                            finalPromises.push(new Promise((innerResolve, innerReject) => {
                                innerResolve();
                            }));
                        }

                        Promise.all(finalPromises).then(() => {
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