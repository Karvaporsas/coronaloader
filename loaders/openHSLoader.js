/*jslint node: true */
/*jshint esversion: 6 */
'use strict';
const moment = require('moment');
const database = require('../database');
const utils = require('./../utils');
const Axios = require('axios');
const DEBUG_MODE = process.env.DEBUG_MODE === 'ON';
const _inboundDateFormat = utils.getDefaultInboundDateTimeFormat();

function _isCorrectCaseTimeValue(c) {
    return moment(c.date, _inboundDateFormat).format('HH:mm:ss') == '15:00:00';
}

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
        const vaccinationOptions = {
            method: 'GET',
            url: 'https://w3qa5ydb4l.execute-api.eu-west-1.amazonaws.com/prod/finnishVaccinationData'
        };

        var results = {
            confirmed: [],
            deaths: [],
            recovered: [],
            hospitalizations: [],
            vaccinations: []
        };
        var currentDate = new Date();
        var currentDateString = currentDate.toLocaleDateString() + ' ' + currentDate.toLocaleTimeString();
        Axios(dataOptions).then((result) => {
            var data = result.data;

            for (const confirmedCase of data.confirmed) {
                if (!_isCorrectCaseTimeValue(confirmedCase)) continue;

                results.confirmed.push({
                    id: confirmedCase.id,
                    date: moment(confirmedCase.date, _inboundDateFormat).format(utils.getDateTimeFormat()),
                    healthCareDistrict: confirmedCase.healthCareDistrict || null,
                    infectionSourceCountry: confirmedCase.infectionSourceCountry || null,
                    infectionSource: confirmedCase.infectionSource,
                    isremoved: false,
                    insertDate: currentDateString,
                    country: 'FIN'
                });
            }
            for (const death of data.deaths) {
                results.deaths.push({
                    id: death.id,
                    date: moment(death.date, _inboundDateFormat).format(utils.getDateTimeFormat()),
                    healthCareDistrict: death.healthCareDistrict,
                    isremoved: false,
                    insertDate: currentDateString,
                    country: 'FIN'
                });
            }
            for (const curedCase of data.recovered) {
                results.recovered.push({
                    id: curedCase.id,
                    date: moment(curedCase.date, _inboundDateFormat).format(utils.getDateTimeFormat()),
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

            return Axios(vaccinationOptions);
        }).then((vaccinationData) => {
            console.log('Vaccination data');
            console.log(vaccinationData);
            for (const vaccination of vaccinationData.data) {
                vaccination.insertDate = currentDateString;
                vaccination.isremoved = false;
                results.vaccinations.push(vaccination);
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