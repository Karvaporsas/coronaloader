/*jslint node: true */
/*jshint esversion: 6 */
'use strict';
const database = require('../database');
const Axios = require('axios');
const DEBUG_MODE = process.env.DEBUG_MODE === 'ON';

module.exports = {
    load(operation, resolve, reject) {
        if (DEBUG_MODE) {
            console.log(operation);
        }

        const dataOptions = {
            method: 'GET',
            url: `https://w3qa5ydb4l.execute-api.eu-west-1.amazonaws.com/prod/finnishCoronaData/v2`
        };
        const hospitalizationOptions = {
            method: 'GET',
            url: `https://w3qa5ydb4l.execute-api.eu-west-1.amazonaws.com/prod/finnishCoronaHospitalData`
        };

        var results = {
            confirmed: [],
            deaths: [],
            recovered: [],
            hospitalizations: []
        };
        var currentDate = new Date();
        var currentDateString = currentDate.toLocaleDateString() + ' ' + currentDate.toLocaleTimeString();
        Axios(dataOptions).then((result) => {
            var data = result.data;

            for (const confirmedCase of data.confirmed) {
                results.confirmed.push({
                    id: confirmedCase.id,
                    date: confirmedCase.date,
                    healthCareDistrict: confirmedCase.healthCareDistrict || null,
                    infectionSourceCountry: confirmedCase.infectionSourceCountry || 'unknown',
                    infectionSource: confirmedCase.infectionSource,
                    isremoved: false,
                    insertDate: currentDateString,
                    country: 'FIN'
                });
            }
            for (const death of data.deaths) {
                results.deaths.push({
                    id: death.id,
                    date: death.date,
                    healthCareDistrict: death.healthCareDistrict,
                    isremoved: false,
                    insertDate: currentDateString,
                    country: 'FIN'
                });
            }
            for (const curedCase of data.recovered) {
                results.recovered.push({
                    id: curedCase.id,
                    date: curedCase.date,
                    healthCareDistrict: curedCase.healthCareDistrict,
                    isremoved: false,
                    insertDate: currentDateString,
                    country: 'FIN'
                });
            }

            return Axios(hospitalizationOptions);
        }).then((hospitalizationData) => {
            for (const hospitalization of hospitalizationData.data.hospitalised) { //sic
                hospitalization.insertDate = currentDateString;
                hospitalization.isremoved = false;
                results.hospitalizations.push(hospitalization);
            }
            return database.updateOperation(operation);
        }).then(() => {
            resolve({status: 1, cases: results, type: operation.type, message: 'All done'});
        }).catch(e => {
            console.log('Error getting cases from vendor');
            console.log(e);
            reject(e);
        });
    }
};