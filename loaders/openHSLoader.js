/*jslint node: true */
/*jshint esversion: 6 */
'use strict';
const rp = require('request-promise');
const database = require('../database');
const DEBUG_MODE = process.env.DEBUG_MODE === 'ON';

module.exports = {
    load(operation, resolve, reject) {
        if (DEBUG_MODE) {
            console.log(operation);
        }

        var options = {
            method: 'GET',
            url: `https://w3qa5ydb4l.execute-api.eu-west-1.amazonaws.com/prod/finnishCoronaData`
        };

        rp(options, (error, response, body) => {
            if (error) {
                console.log('Error getting quotes');
                console.log(error);
                reject(error);
            } else {
                var results = {
                    confirmed: [],
                    deaths: [],
                    recovered: []
                };

                body = JSON.parse(body);

                for (const confirmedCase of body.confirmed) {
                    results.confirmed.push({
                        id: parseInt(confirmedCase.id),
                        acqDate: confirmedCase.date,
                        healthCareDistrict: confirmedCase.healthCareDistrict,
                        infectionSourceCountry: confirmedCase.infectionSourceCountry || 'unknown',
                        infectionSource: confirmedCase.infectionSource,
                        insertDate: new Date().toLocaleDateString() + ' ' + new Date().toLocaleTimeString()
                    });
                }
                for (const death of body.deaths) {
                    results.deaths.push({
                        id: parseInt(death.id),
                        date: death.date,
                        healthCareDistrict: death.healthCareDistrict,
                        insertDate: new Date().toLocaleDateString() + ' ' + new Date().toLocaleTimeString()
                    });
                }
                for (const curedCase of body.recovered) {
                    results.recovered.push({
                        id: parseInt(curedCase.id),
                        date: curedCase.date,
                        healthCareDistrict: curedCase.healthCareDistrict,
                        insertDate: new Date().toLocaleDateString() + ' ' + new Date().toLocaleTimeString()
                    });
                }

                database.updateOperation(operation).then(() => {
                    resolve({status: 1, cases: results, message: 'All done'});
                }).catch((e) => {
                    reject(e);
                });
            }
        }).catch(e => {
            console.log('Error getting cases');
            console.log(e);
            reject(e);
        });
    }
};