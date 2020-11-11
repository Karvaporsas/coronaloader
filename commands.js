/*jslint node: true */
/*jshint esversion: 6 */
'use strict';

const loadHandler = require('./handlers/loadHandler');
const chartHandler = require('./handlers/chartHandler');
const utils = require('./utils');
const database = require('./database');
const DATASOURCE_FOR_CHARTS = process.env.DATASOURCE_FOR_CHARTS || 'DB';

module.exports = {
    process(command, data) {
        switch (command) {
            case 'load':
            case 'autoload':
                return loadHandler.autoLoad();
            case 'update':
                return loadHandler.autoLoad(true, data.tresholdDays);
            case 'createcharts':
                return chartHandler.createCharts(data.hcd || '');
            case 'createallcharts':
                return new Promise((resolve, reject) => {
                    database.getConfirmedCases(DATASOURCE_FOR_CHARTS).then((confirmedCases) => {
                        var promises = [];
                        var districts = utils.getHCDNames();
                        var timeout = 1500;

                        for (const hcd of districts) {
                            console.log(`Iterating now: ${hcd}`);
                            promises.push( chartHandler.createCharts(hcd, timeout, confirmedCases));
                            timeout += 1500;
                        }
                        promises.push(chartHandler.createCharts('', timeout));

                        return Promise.all(promises);
                    }).then(() => {
                        resolve();
                    });
                });


            case 'createhospitalization':
                return chartHandler.createHospitalizationCharts();
            default:
                return new Promise((resolve, reject) => {
                    reject('Unknown comand');
                });
        }
    },
};